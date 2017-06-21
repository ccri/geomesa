/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.{MessageTypes, SharedState}

/**
  * Consumes from kakfa and populates the local cache
  *   1. reads offsets stored in zk on startup
  *   2. scheduled repeating - reads features from kafka, adds to in-memory cache
  *   3. listens for offsets change in zk, removes expired features from in-memory cache
  *
  * @param offsetManager offset manager
  * @param serializer feature serializer
  * @param state shared state
  * @param config kafka consumer config
  * @param topic kafka topic
  */
class KafkaCacheLoader(offsetManager: OffsetManager,
                       serializer: KryoFeatureSerializer,
                       state: SharedState,
                       config: Map[String, String],
                       topic: String)
    extends Runnable with OffsetListener with Closeable with LazyLogging {

  private val running = new AtomicBoolean(false)

  private val offsets = new ConcurrentHashMap[Int, Long]()

  // TODO allow for multiple consumers if there are multiple partitions

  private val consumer = KafkaStore.consumer(config, topic, offsetManager, state)

  // register as a listener for offset changes
  offsetManager.addOffsetListener(topic, this)

  private val schedule = KafkaStore.executor.scheduleWithFixedDelay(this, 0L, 100L, TimeUnit.MILLISECONDS)

  override def run(): Unit = {
    running.set(true)
    try {
      val records = consumer.poll(0).iterator
      while (records.hasNext) {
        val record = records.next()
        val (time, action) = KafkaStore.deserializeKey(record.key)
        val feature = serializer.deserialize(record.value)
        action match {
          case MessageTypes.Write  =>
            logger.trace(s"Adding [${record.partition}:${record.offset}] created at ${new DateTime(time, DateTimeZone.UTC)}: $feature")
            state.add(feature, record.partition, record.offset, time)

          case MessageTypes.Delete =>
            logger.trace(s"Deleting [${record.partition}:${record.offset}] created at ${new DateTime(time, DateTimeZone.UTC)}: $feature")
            state.delete(feature, record.partition, record.offset, time)

          case _ => logger.error(s"Unhandled message type: $action")
        }
      }
      // we commit the offsets so that the next poll doesn't return the same records
      // on init we roll back to the last offsets persisted to storage
      consumer.commitSync()
    } finally {
      running.set(false)
    }
  }

  override def offsetsChanged(updated: Seq[(Int, Long)]): Unit = {
    // remove the expired features from the cache
    updated.foreach { case (partition, offset) =>
      var current = offsets.get(partition)
      if (current < offset) {
        offsets.put(partition, offset)
        do {
          state.remove(partition, current)
          current += 1
        } while (current < offset)
      }
    }
  }

  override def close(): Unit = {
    consumer.wakeup()
    schedule.cancel(true)
    offsetManager.removeOffsetListener(topic, this)
    // ensure run has finished so that we don't get ConcurrentAccessExceptions closing the consumer
    while (running.get) {
      Thread.sleep(10)
    }
    consumer.close()
  }
}
