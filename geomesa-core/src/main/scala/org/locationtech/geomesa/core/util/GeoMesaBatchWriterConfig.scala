package org.locationtech.geomesa.core.util

import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriterConfig

import scala.util.Try

object GeoMesaBatchWriterConfig extends Logging {
  val WRITERLATENCYSECONDS = "geomesa.batchwriter.latency.seconds"  // Measured in millis
  val WRITERLATENCYMILLIS  = "geomesa.batchwriter.latency.millis"   // Measured in millis
  val WRITERMEMORY         = "geomesa.batchwriter.memory"           // Measured in bytes
  val WRITERTHREADS        = "geomesa.batchwriter.maxthreads"
  val WRITETIMEOUT         = "geomesa.batchwriter.timeout.seconds"  // Timeout measured in seconds.  Likely unnecessary.

  val DEFAULTLATENCY   = 10000l  // 10 seconds
  val DEFAULTMAXMEMORY = 1000000 // 1 megabyte
  val DEFAULTTHREADS   = 10

  private lazy val bwc = buildBWC

  protected [util] def fetchProperty(prop: String): Option[Long] =
    for {
      p <- Option(System.getProperty(prop))
      l <- Try(java.lang.Long.parseLong(p)).toOption
    } yield l


  def buildBWC: BatchWriterConfig = {
    val bwc = new BatchWriterConfig

    fetchProperty(WRITERLATENCYSECONDS).map { latency =>
      logger.info(s"GeoMesaBatchWriter config: maxLatency set to $latency seconds.")
      bwc.setMaxLatency(latency, TimeUnit.SECONDS)
    }

    fetchProperty(WRITERLATENCYMILLIS).map { latency =>
      logger.info(s"GeoMesaBatchWriter config: maxLatency set to $latency milliseconds.")
      bwc.setMaxLatency(latency, TimeUnit.MILLISECONDS)
    }

    fetchProperty(WRITERMEMORY).map { memory =>
      logger.info(s"GeoMesaBatchWriter config: maxMemory set to $memory bytes.")
      bwc.setMaxMemory(memory)
    }
    fetchProperty(WRITERTHREADS).map { threads =>
      logger.info(s"GeoMesaBatchWriter config: maxWriteThreads set to $threads.")
      bwc.setMaxWriteThreads(threads.toInt)
    }
    fetchProperty(WRITETIMEOUT).map { timeout =>
      logger.info(s"GeoMesaBatchWriter config: maxTimeout set to $timeout seconds.")
      bwc.setTimeout(timeout, TimeUnit.SECONDS)
    }

    bwc
  }

  def apply(): BatchWriterConfig = bwc
}