/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.time.Clock
import java.util
import java.util.concurrent._
import java.util.{Comparator, Properties, UUID}

import com.google.common.primitives.Longs
import com.google.common.util.concurrent.MoreExecutors
import kafka.admin.AdminUtils
import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter
import org.locationtech.geomesa.index.utils.{ExplainLogging, Explainer}
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.{MessageTypes, SharedState}
import org.locationtech.geomesa.lambda.stream.{OffsetManager, TransientStore}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.io.{CloseWithLogging, WithClose}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.mutable.ArrayBuffer

class KafkaStore(ds: DataStore,
                 val sft: SimpleFeatureType,
                 authProvider: Option[AuthorizationsProvider],
                 offsetManager: OffsetManager,
                 producer: Producer[Array[Byte], Array[Byte]],
                 consumerConfig: Map[String, String],
                 config: LambdaConfig)
                (implicit clock: Clock = Clock.systemUTC()) extends TransientStore {

  private val topic = KafkaStore.topic(config.zkNamespace, sft)

  private val state = {
    // shared map that stores our current features by feature id
    val features = new ConcurrentHashMap[String, SimpleFeature]
    // map of (partition, offset) -> (feature id, expires)
    val offsets = new ConcurrentHashMap[(Int, Long), (SimpleFeature, Long)]
    SharedState(features, offsets, ArrayBuffer.empty)
  }

  private val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withUserData)

  private val queryRunner = new KafkaQueryRunner(state.features, authProvider)

  private val loader = new KafkaCacheLoader(offsetManager, serializer, state, consumerConfig, topic, config.partitions)
  private val persistence = new DataStorePersistence(ds, sft, offsetManager, state, topic, config.expiry, config.persist)

  override def createSchema(): Unit = {}

  override def removeSchema(): Unit = {
    offsetManager.deleteOffsets(topic)
    val security = SystemProperty("geomesa.zookeeper.security.enabled").option.exists(_.toBoolean)
    val zkUtils = ZkUtils(config.zookeepers, 3000, 3000, security)
    try {
      AdminUtils.deleteTopic(zkUtils, topic)
    } finally {
      zkUtils.close()
    }
  }

  override def read(filter: Option[Filter] = None,
                    transforms: Option[Array[String]] = None,
                    hints: Option[Hints] = None,
                    explain: Explainer = new ExplainLogging): Iterator[SimpleFeature] = {
    val query = new Query()
    filter.foreach(query.setFilter)
    transforms.foreach(query.setPropertyNames)
    hints.foreach(query.setHints) // note: we want to share the hints object
    queryRunner.runQuery(sft, query, explain)
  }

  override def write(feature: SimpleFeature): Unit = {
    val serialized = serializer.serialize(GeoMesaFeatureWriter.featureWithFid(sft, feature))
    producer.send(new ProducerRecord(topic, KafkaStore.serializeKey(clock.millis(), MessageTypes.Write), serialized))
  }

  override def delete(feature: SimpleFeature): Unit = {
    import org.locationtech.geomesa.filter.ff
    // send a message to delete from all transient stores
    val serialized = serializer.serialize(feature)
    producer.send(new ProducerRecord(topic, KafkaStore.serializeKey(clock.millis(), MessageTypes.Delete), serialized))
    // also delete from persistent store
    if (config.persist) {
      val filter = ff.id(ff.featureId(feature.getID))
      WithClose(ds.getFeatureWriter(sft.getTypeName, filter, Transaction.AUTO_COMMIT)) { writer =>
        while (writer.hasNext) {
          writer.next()
          writer.remove()
        }
      }
    }
  }

  override def persist(): Unit = persistence.run()

  override def close(): Unit = {
    CloseWithLogging(loader)
    CloseWithLogging(persistence)
  }
}

object KafkaStore {

  private [stream] val executor =
    MoreExecutors.getExitingScheduledExecutorService(
      Executors.newScheduledThreadPool(2).asInstanceOf[ScheduledThreadPoolExecutor])
  sys.addShutdownHook { executor.shutdownNow(); executor.awaitTermination(1, TimeUnit.SECONDS) }

  private val expiryComparator = new Comparator[(Long, SimpleFeature, Long)]() {
    override def compare(o1: (Long, SimpleFeature, Long), o2: (Long, SimpleFeature, Long)): Int =
      java.lang.Long.compare(o1._1, o2._1)
  }

  object MessageTypes {
    val Write:  Byte = 0
    val Delete: Byte = 1
  }

  def topic(ns: String, sft: SimpleFeatureType): String =
    s"${ns}_${sft.getTypeName}".replaceAll("[^a-zA-Z0-9_\\-]", "_")

  def producer(connect: Map[String, String]): Producer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    // set some defaults but allow them to be overridden
    props.put("acks", "1") // mix of reliability and performance
    props.put("retries", Int.box(3))
    props.put("linger.ms", Int.box(3)) // helps improve batching at the expense of slight delays in write
    connect.foreach { case (k, v) => props.put(k, v) }
    props.put("key.serializer", classOf[ByteArraySerializer].getName)
    props.put("value.serializer", classOf[ByteArraySerializer].getName)
    new KafkaProducer[Array[Byte], Array[Byte]](props)
  }

  def consumer(connect: Map[String, String], group: String): Consumer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    connect.foreach { case (k, v) => props.put(k, v) }
    props.put("group.id", group)
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", classOf[ByteArrayDeserializer].getName)
    props.put("value.deserializer", classOf[ByteArrayDeserializer].getName)
    new KafkaConsumer[Array[Byte], Array[Byte]](props)
  }

  // creates a consumer and sets to the latest offsets
  private [kafka] def consumers(connect: Map[String, String],
                                topic: String,
                                manager: OffsetManager,
                                state: SharedState,
                                parallelism: Int): Seq[Consumer[Array[Byte], Array[Byte]]] = {
    val group = UUID.randomUUID().toString
    val topics = java.util.Arrays.asList(topic)

    Seq.fill(parallelism) {
      val consumer = KafkaStore.consumer(connect, group)
      consumer.subscribe(topics, new OffsetRebalancer(consumer, topic, manager, state))
      consumer
    }
  }

  private [kafka] def serializeKey(time: Long, action: Byte): Array[Byte] = {
    val result = Array.ofDim[Byte](9)

    result(0) = ((time >> 56) & 0xff).asInstanceOf[Byte]
    result(1) = ((time >> 48) & 0xff).asInstanceOf[Byte]
    result(2) = ((time >> 40) & 0xff).asInstanceOf[Byte]
    result(3) = ((time >> 32) & 0xff).asInstanceOf[Byte]
    result(4) = ((time >> 24) & 0xff).asInstanceOf[Byte]
    result(5) = ((time >> 16) & 0xff).asInstanceOf[Byte]
    result(6) = ((time >> 8)  & 0xff).asInstanceOf[Byte]
    result(7) = (time & 0xff        ).asInstanceOf[Byte]
    result(8) = action

    result
  }

  private [kafka] def deserializeKey(key: Array[Byte]): (Long, Byte) = (Longs.fromByteArray(key), key(8))

  // TODO ensure a feature goes to the same partition

  private [kafka] class OffsetRebalancer(consumer: Consumer[Array[Byte], Array[Byte]],
                                         topic: String,
                                         manager: OffsetManager,
                                         state: SharedState) extends ConsumerRebalanceListener {

    override def onPartitionsRevoked(topicPartitions: util.Collection[TopicPartition]): Unit = {}

    override def onPartitionsAssigned(topicPartitions: util.Collection[TopicPartition]): Unit = {
      import scala.collection.JavaConversions._

      // ensure we have queues for each partition
      state.synchronized {
        topicPartitions.foreach { tp =>
          while (state.expiry.length <= tp.partition) {
            state.expiry.append(new PriorityBlockingQueue[(Long, SimpleFeature, Long)](1000, expiryComparator))
          }
        }
      }

      // read our last committed offsets and seek to them
      val lastRead = manager.getOffsets(topic)
      topicPartitions.foreach { tp =>
        lastRead.find(_._1 == tp.partition()) match {
          case Some((_, offset)) => consumer.seek(tp, offset)
          case None => consumer.seekToBeginning(tp)
        }
      }
    }
  }
  /**
    * Locally cached features
    *
    * @param features map of feature id -> feature
    * @param offsets map of (partition, offset) -> (feature id, create time)
    * @param expiry array, indexed by partition, of queues of (create time, feature, offset)
    */
  private [kafka] case class SharedState(features: ConcurrentMap[String, SimpleFeature],
                                         offsets: ConcurrentMap[(Int, Long), (SimpleFeature, Long)],
                                         expiry: ArrayBuffer[PriorityBlockingQueue[(Long, SimpleFeature, Long)]]) {

    def add(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
      offsets.put((partition, offset), (f, created))
      features.put(f.getID, f)
      expiry(partition).offer((created, f, offset))
    }

    def delete(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = features.remove(f.getID)

    def remove(partition: Int, offset: Long): Unit = {
      val featureAndExpires = offsets.remove((partition, offset))
      if (featureAndExpires != null) {
        val feature = featureAndExpires._1
        // only remove if there haven't been additional updates
        if (feature.eq(features.get(feature.getID))) {
          features.remove(feature.getID)
        }
        expiry(partition).remove((featureAndExpires._2, feature, offset))
      }
    }
  }
}
