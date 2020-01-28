/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor

import java.io.{InterruptedIOException, _}
import java.util.Collections
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.google.protobuf.{ByteString, RpcCallback, RpcController, Service}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.client.coprocessor.Batch.Call
import org.apache.hadoop.hbase.client.{Connection, Scan}
import org.apache.hadoop.hbase.coprocessor.{CoprocessorException, CoprocessorService, RegionCoprocessorEnvironment}
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.protobuf.{ProtobufUtil, ResponseConverter}
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.hbase.{Coprocessor, CoprocessorEnvironment, TableName}
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor.Aggregator
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseAggregator
import org.locationtech.geomesa.hbase.coprocessor.utils.{GeoMesaHBaseCallBack, GeoMesaHBaseRpcController}
import org.locationtech.geomesa.hbase.proto.GeoMesaProto
import org.locationtech.geomesa.hbase.proto.GeoMesaProto.{GeoMesaCoprocessorRequest, GeoMesaCoprocessorResponse, GeoMesaCoprocessorService}
import org.locationtech.geomesa.index.iterators.AggregatingScan.Configuration.CqlOpt
import org.locationtech.geomesa.index.iterators.AggregatingScan.Result
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.io.WithClose

import scala.util.control.NonFatal

class GeoMesaCoprocessor extends GeoMesaCoprocessorService with Coprocessor with CoprocessorService with LazyLogging {

  import scala.collection.JavaConverters._

  private var env: RegionCoprocessorEnvironment = _

  @throws[IOException]
  override def start(env: CoprocessorEnvironment): Unit = {
    env match {
      case e: RegionCoprocessorEnvironment => this.env = e
      case _ => throw new CoprocessorException("Must be loaded on a table region!")
    }
  }

  @throws[IOException]
  override def stop(coprocessorEnvironment: CoprocessorEnvironment): Unit = {
  }

  override def getService: Service = this

  override def getResult(
      controller: RpcController,
      request: GeoMesaProto.GeoMesaCoprocessorRequest,
      done: RpcCallback[GeoMesaProto.GeoMesaCoprocessorResponse]): Unit = {

    val queryNumber = GeoMesaCoprocessor.requestNumber.incrementAndGet

    val results = GeoMesaCoprocessorResponse.newBuilder()

    try {
            logger.debug(s"Starting to process request $queryNumber.")
            GeoMesaCoprocessor.logMemoryInfo(s"start of request $queryNumber")
      val options = GeoMesaCoprocessor.deserializeOptions(request.getOptions.toByteArray)
      val timeout = options.get(GeoMesaCoprocessor.TimeoutOpt).map(_.toLong + System.currentTimeMillis())
      if (!controller.isCanceled) {
        val clas = options(GeoMesaCoprocessor.AggregatorClass)
        logger.debug(s"Instantiating $clas for request $queryNumber.")
        WithClose(Class.forName(clas).newInstance().asInstanceOf[Aggregator]) { aggregator =>

          //logger.debug(s"Initializing aggregator $aggregator with options ${options.mkString(", ")}")
          aggregator.init(options)

          val scan = ProtobufUtil.toScan(ClientProtos.Scan.parseFrom(Base64.decode(options(GeoMesaCoprocessor.ScanOpt))))
          scan.setFilter(FilterList.parseFrom(Base64.decode(options(GeoMesaCoprocessor.FilterOpt))))

          // enable visibilities by delegating to the region server configured coprocessors
          env.getRegion.getCoprocessorHost.preScannerOpen(scan)

          // TODO: Explore use of MultiRangeFilter
          WithClose(env.getRegion.getScanner(scan)) { scanner =>
            logger.debug(s"Starting scan for request $queryNumber.")
            aggregator.setScanner(scanner)
            var done = false
            while (!done) {
              logger.trace(s"Running batch on aggregator $aggregator")
              val agg = aggregator.aggregate()
              if (agg == null) { done = true } else {
                results.addPayload(ByteString.copyFrom(agg))
                if (controller.isCanceled) {
                  logger.warn(s"Stopping aggregator $aggregator for request $queryNumber due to controller being cancelled")
                  done = true
                } else if (timeout.exists(_ < System.currentTimeMillis())) {
                  logger.warn(s"Stopping aggregator $aggregator for request $queryNumber due to timeout of ${options(GeoMesaCoprocessor.TimeoutOpt)}ms")
                  done = true
                }
              }
            }
            logger.debug(s"Closing scanner for request $queryNumber.")  // Closing as we pass out of the WithClose block
          }
        }
      } else {
        logger.debug(s"Request #$queryNumber timed out.")
      }
    } catch {
      case e: InterruptedException   =>
        logger.debug(s"Got exception while handling request $queryNumber " + e.getMessage)
        e.printStackTrace()
      case e: InterruptedIOException => // stop processing, but don't return an error to prevent retries
        logger.debug(s"Got exception while handling request $queryNumber " + e.getMessage)
        e.printStackTrace()
      case e: IOException => ResponseConverter.setControllerException(controller, e)
        logger.debug(s"Got exception while handling request $queryNumber " + e.getMessage)
        e.printStackTrace()
      case NonFatal(e) => ResponseConverter.setControllerException(controller, new IOException(e))
        logger.debug(s"Got exception while handling request $queryNumber " + e.getMessage)
        e.printStackTrace()
    }

    logger.debug(
      s"Results for request $queryNumber have total size: ${results.getPayloadList.asScala.map(_.size()).sum}" +
          s"\n\tBatch sizes: ${results.getPayloadList.asScala.map(_.size()).mkString(", ")}")
    GeoMesaCoprocessor.logMemoryInfo(s"end of request $queryNumber")

    done.run(results.build)
  }
}

object GeoMesaCoprocessor extends LazyLogging {

  val AggregatorClass = "geomesa.hbase.aggregator.class"

  private type Aggregator = HBaseAggregator[_ <: Result]

  private val FilterOpt  = "filter"
  private val ScanOpt    = "scan"
  val TimeoutOpt = "timeout"

  private val requestNumber = new AtomicLong(0)

  lazy private val executor: ScheduledExecutorService = new ScheduledThreadPoolExecutor(1)
  executor.scheduleWithFixedDelay(runnable, 0, 10, TimeUnit.SECONDS)

  lazy val runnable = new Runnable {
    override def run(): Unit = logMemoryInfo() //logVerboseArrow()
  }

  def logMemoryInfo(message: String = s"timestamp ${System.currentTimeMillis()}"): Unit = {
    // Allocate state at (

    logger.debug(s"Arrow Allocator state at $message: ${org.locationtech.geomesa.arrow.allocator.toString}")
    logger.debug(s"Direct Memory state at $message: MAX_DIRECT_MEMORY: ${io.netty.util.internal.PlatformDependent.maxDirectMemory()} DIRECT_MEMORY_COUNTER: ${getNettyMemoryCounter()}")
  }

  def logVerboseArrow(message: String = s"timestamp ${System.currentTimeMillis()}") = {
    logMemoryInfo(message)
    logger.debug(s"Arrow Allocator state at $message: ${org.locationtech.geomesa.arrow.allocator.toVerboseString}")
  }


  def getNettyMemoryCounter(): Long = {
    try {
      val clazz = try {
        Class.forName("io.netty.util.internal.PlatformDependent")
      } catch {
        case _: Throwable =>
          Class.forName("org.locationtech.geomesa.accumulo.shade.io.netty.util.internal.PlatformDependent")
      }
      val field = clazz.getDeclaredField("DIRECT_MEMORY_COUNTER")
      field.setAccessible(true)
      field.get(clazz).asInstanceOf[AtomicLong].get

    } catch {
      case _: Throwable => 0
    }
  }

  private def deserializeOptions(bytes: Array[Byte]): Map[String, String] = {
    WithClose(new ByteArrayInputStream(bytes)) { bais =>
      WithClose(new ObjectInputStream(bais)) { ois =>
        ois.readObject.asInstanceOf[Map[String, String]]
      }
    }
  }

  @throws[IOException]
  private def serializeOptions(map: Map[String, String]): Array[Byte] = {
    WithClose(new ByteArrayOutputStream) { baos =>
      WithClose(new ObjectOutputStream(baos)) { oos =>
        oos.writeObject(map)
        oos.flush()
      }
      baos.toByteArray
    }
  }

  /**
   * Executes a geomesa coprocessor
   *
   * @param connection connection
   * @param table table to execute against
   * @param scan scan to execute
   * @param options configuration options
   * @param threads number of threads to use
   * @return serialized results
    */
  def execute(
      connection: Connection,
      table: TableName,
      scan: Scan,
      options: Map[String, String],
      threads: Int): CloseableIterator[ByteString] = new RpcIterator(connection, table, scan, options, threads)

  /**
   * Timeout configuration option
   *
   * @param millis milliseconds
   * @return
   */
  def timeout(millis: Long): (String, String) = TimeoutOpt -> (millis + System.currentTimeMillis()).toString

  /**
   * Closeable iterator implementation for invoking coprocessor rpcs
   *
   * @param table hbase table
   * @param scan scan
   * @param options coprocessor options
   */
  class RpcIterator(
      connection: Connection,
      table: TableName,
      scan: Scan,
      options: Map[String, String],
      threads: Int
    ) extends CloseableIterator[ByteString] {

    private val pool = new ThreadPoolExecutor(1, threads, 60, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable])
    private val htable = connection.getTable(table, pool)
    private val closed = new AtomicBoolean(false)

    private val request = {
      val opts = options
          .updated(FilterOpt, Base64.encodeBytes(scan.getFilter.toByteArray))
          .updated(ScanOpt, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))
      GeoMesaCoprocessorRequest.newBuilder().setOptions(ByteString.copyFrom(serializeOptions(opts))).build()
    }

    private val callable = new Call[GeoMesaCoprocessorService, java.util.List[ByteString]]() {
      override def call(instance: GeoMesaCoprocessorService): java.util.List[ByteString] = {
        if (closed.get) { Collections.emptyList() } else {
          val controller: RpcController = new GeoMesaHBaseRpcController()
          val callback = new RpcCallbackImpl()
          // note: synchronous call
          try { instance.getResult(controller, request, callback) } catch {
            case _: InterruptedException | _: InterruptedIOException | _: CancellationException =>
              logger.warn("Cancelling remote coprocessor call")
              controller.startCancel()
          }

          if (controller.failed()) {
            logger.error(s"Controller failed with error:\n${controller.errorText()}")
          }

          callback.get()
        }
      }
    }

    lazy private val result: Iterator[ByteString] = if (closed.get) { Iterator.empty } else {
      val callBack = new GeoMesaHBaseCallBack()
      try { htable.coprocessorService(classOf[GeoMesaCoprocessorService], null, null, callable, callBack) } catch {
        case e @ (_ :InterruptedException | _ :InterruptedIOException) =>
          logger.warn("Interrupted executing coprocessor query:", e)
      }
      callBack.getResult
    }

    override def hasNext: Boolean = result.hasNext

    override def next(): ByteString = result.next

    override def close(): Unit = {
      closed.set(true)
      pool.shutdownNow()
      htable.close()
        println("Do we make it here?")
        logger.error("Do we make it here?")
    }
  }

  /**
    * Unsynchronized rpc callback
    */
  class RpcCallbackImpl extends RpcCallback[GeoMesaCoprocessorResponse] {

    private var result: java.util.List[ByteString] = _

    def get(): java.util.List[ByteString] = result

    override def run(parameter: GeoMesaCoprocessorResponse): Unit =
      result = Option(parameter).map(_.getPayloadList).orNull
  }
}
