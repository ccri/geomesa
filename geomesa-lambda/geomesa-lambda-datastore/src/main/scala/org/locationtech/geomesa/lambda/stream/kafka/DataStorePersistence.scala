/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.io.Closeable
import java.time.Clock
import java.util.concurrent.{PriorityBlockingQueue, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Transaction}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.SharedState
import org.locationtech.geomesa.utils.geotools.FeatureUtils
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable.ArrayBuffer

/**
  * Persists expired entries to the data store
  *   1. checks for expired entries
  *   2. gets zk lock
  *   3. gets offsets from zk
  *   4. writes expired entries to data store
  *   5. updates offsets in zk
  *   6. releases zk lock
  *
  * @param ds data store to write to
  * @param sft simple feature type
  * @param offsetManager offset manager
  * @param state shared state
  * @param topic kafka topic
  * @param ageOffMillis age off for expiring features
  * @param clock clock used for checking expiration
  */
class DataStorePersistence(ds: DataStore,
                           sft: SimpleFeatureType,
                           offsetManager: OffsetManager,
                           state: SharedState,
                           topic: String,
                           ageOffMillis: Long,
                           persistExpired: Boolean)
                          (implicit clock: Clock = Clock.systemUTC())
    extends Runnable with Closeable with LazyLogging {

  private val schedule = KafkaStore.executor.scheduleWithFixedDelay(this, 0L, 60L, TimeUnit.SECONDS)

  override def run(): Unit = {
    val expired = state.synchronized(state.expiry.zipWithIndex).filter(e => checkPartition(e._1))
    logger.trace(s"Found ${expired.length} partitions with expired entries in $topic")
    // lock per-partition to allow for multiple write threads
    expired.foreach { case (queue, partition) =>
      // if we don't get the lock just try again next run
      logger.trace(s"Acquiring lock for $topic:$partition")
      offsetManager.acquireLock(topic, partition, 1000L) match {
        case None => logger.trace(s"Could not acquire lock for $topic:$partition within timeout")
        case Some(lock) =>
          try {
            logger.trace(s"Acquired lock for $topic:$partition")
            persist(partition, queue, clock.millis() - ageOffMillis)
          } finally {
            lock.release()
            logger.trace(s"Released lock for $topic:$partition")
          }
      }
    }
  }

  private def checkPartition(expiry: PriorityBlockingQueue[(Long, SimpleFeature, Long)]): Boolean = {
    expiry.peek() match {
      case null => false
      case (created, _, _) => (clock.millis() - ageOffMillis) > created
    }
  }

  private def persist(partition: Int, queue: PriorityBlockingQueue[(Long, SimpleFeature, Long)], expiry: Long): Unit = {
    import org.locationtech.geomesa.filter.ff

    val expired = ArrayBuffer.empty[(Long, SimpleFeature, Long)]

    var loop = true
    while (loop) {
      queue.poll() match {
        case null => loop = false
        case f if f._1 > expiry => queue.offer(f); loop = false
        case f => expired.append(f)
      }
    }

    logger.trace(s"Found expired entries for $topic:$partition :\n\t" +
        expired.map { case (created, f, o) => s"offset $o: $f created at ${new DateTime(created, DateTimeZone.UTC)}" }.mkString("\n\t"))

    if (expired.nonEmpty) {
      val offsets = scala.collection.mutable.Map(offsetManager.getOffsets(topic): _*)
      logger.trace(s"Last persisted offsets for $topic: ${offsets.map { case (p, o) => s"$p:$o"}.mkString(",") }")
      // check that features haven't been persisted yet, haven't been deleted, and have no additional updates pending
      val toPersist = {
        val filtered = expired.filter { case (_, f, offset) =>
          if (offsets.get(partition).forall(_ <= offset)) {
            // update the offsets we will write
            offsets(partition) = offset + 1
            // ensure not deleted and no additional updates pending
            f.eq(state.features.get(f.getID))
          } else {
            false
          }
        }
        val byId = filtered.map(f => (f._2.getID,  f))
        scala.collection.mutable.Map(byId: _*) // note: more recent values will overwrite older ones
      }

      logger.trace(s"Entries to persist for $topic:$partition:\n\t" +
          toPersist.values.toSeq.sortBy(_._3).map {
            case (created, f, o) => s"offset $o: $f created at ${new DateTime(created, DateTimeZone.UTC)}"
          }.mkString("\n\t"))

      // TODO handle failures writing
      if (toPersist.nonEmpty) {
        if (persistExpired) {
          // do an update query first
          val filter = ff.id(toPersist.keys.map(ff.featureId).toSeq: _*)
          val t = Transaction.AUTO_COMMIT
          WithClose(ds.getFeatureWriter(sft.getTypeName, filter, t)) { writer =>
            while (writer.hasNext) {
              val next = writer.next()
              toPersist.get(next.getID).foreach { case (created, updated, offset) =>
                logger.trace(s"Updating [$topic:$partition:$offset] $updated created at ${new DateTime(created, DateTimeZone.UTC)}")
                next.setAttributes(updated.getAttributes)
                next.getUserData.putAll(updated.getUserData)
                next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(updated.getID)
                next.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
                writer.write()
                toPersist.remove(updated.getID)
                state.remove(partition, offset)
              }
            }
          }
          // if any weren't updates, add them as inserts
          if (toPersist.nonEmpty) {
            WithClose(ds.getFeatureWriterAppend(sft.getTypeName, t)) { writer =>
              toPersist.values.foreach { case (created, updated, offset) =>
                logger.trace(s"Appending [$topic:$partition:$offset] $updated created at ${new DateTime(created, DateTimeZone.UTC)}")
                FeatureUtils.copyToWriter(writer, updated, useProvidedFid = true)
                writer.write()
                state.remove(partition, offset)
              }
            }
          }
        } else {
          logger.trace(s"Persist disabled for $topic")
          toPersist.values.foreach { case (_, _, offset) => state.remove(partition, offset) }
        }
        logger.trace(s"Committing offsets for $topic: ${offsets.map { case (p, o) => s"$p:$o"}.mkString(",")}")
        offsetManager.setOffsets(topic, offsets.toSeq)
      }
    }
  }

  override def close(): Unit = schedule.cancel(true)
}
