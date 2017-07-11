/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.util.Comparator
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{ConcurrentHashMap, PriorityBlockingQueue}

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
import org.opengis.feature.simple.SimpleFeature

import scala.collection.mutable.ArrayBuffer

/**
  * Locally cached features
  */
class SharedState(topic: String, partitions: Int, expire: Boolean) extends OffsetListener with LazyLogging {

  // map of feature id -> current feature
  private val features = new ConcurrentHashMap[String, SimpleFeature]

  // technically we should synchronize all access to the following arrays, since we expand them if needed;
  // however, in normal use it will be created up front and then only read.
  // if partitions are added at runtime, we may have synchronization issues...

  // array, indexed by partition, of maps of offset -> feature, for all features we have added
  private val offsets = ArrayBuffer.fill(partitions)(new ConcurrentHashMap[Long, SimpleFeature])
  // last offset that hasn't been expired
  private val validOffsets = ArrayBuffer.fill(partitions)(new AtomicLong(-1L))

  // array, indexed by partition, of queues of (offset, create time, feature), sorted by offset
  private val expiry = ArrayBuffer.fill(partitions)(newQueue)
  private val expiryWithIndex = expiry.zipWithIndex

  def get(id: String): SimpleFeature = features.get(id)

  def all(): Iterator[SimpleFeature] = {
    import scala.collection.JavaConverters._
    features.values.iterator.asScala
  }

  def expired: Seq[(PriorityBlockingQueue[(Long, Long, SimpleFeature)], Int)] = expiryWithIndex

  def add(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    val valid = validOffsets(partition).get
    if (valid <= offset) {
      logger.trace(s"Adding [$partition:$offset] $f created at ${new DateTime(created, DateTimeZone.UTC)}")
      features.put(f.getID, f)
      offsets(partition).put(offset, f)
      if (expire) {
        expiry(partition).offer((offset, created, f))
      }
    } else {
      logger.trace(s"Ignoring [$partition:$offset] $f created at ${new DateTime(created, DateTimeZone.UTC)}, valid offset is $valid")
    }
  }

  def delete(f: SimpleFeature, partition: Int, offset: Long, created: Long): Unit = {
    logger.trace(s"Deleting [$partition:$offset] $f created at ${new DateTime(created, DateTimeZone.UTC)}")
    features.remove(f.getID)
  }

  def partitionAssigned(partition: Int, offset: Long): Unit = synchronized {
    while (expiry.length <= partition) {
      val queue = newQueue
      expiryWithIndex += ((queue, expiry.length))
      expiry += queue
      offsets += new ConcurrentHashMap[Long, SimpleFeature]
      validOffsets += new AtomicLong(-1L)
    }
    if (validOffsets(partition).get < offset) {
      validOffsets(partition).set(offset)
    }
  }

  override def offsetChanged(partition: Int, offset: Long): Unit = {
    var current = validOffsets(partition).get
    validOffsets(partition).set(offset)

    // remove the expired features from the cache
    if (current == -1L) {
      logger.debug(s"Setting initial offset [$topic:$partition:$offset]")
    } else {
      logger.debug(s"Offsets changed for [$topic:$partition]: $current->$offset")
      logger.debug(s"Size of cached state (before removal) for [$topic]: ${debug()}")
      while (current < offset) {
        val feature = offsets(partition).remove(current)
        if (feature == null) {
          logger.trace(s"Tried to remove [$partition:$current], but it was not present in the offsets map")
        } else if (feature.eq(features.get(feature.getID))) {
          // ^ only remove from feature cache if there haven't been additional updates
          features.remove(feature.getID)
          // note: don't remove from expiry queue, it requires a synchronous traversal
          // instead, allow persistence run to remove
        }
        current += 1L
      }
      logger.debug(s"Size of cached state (after removal) for [$topic]: ${debug()}")
    }
  }

  private def debug(): String = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce
    s"features: ${features.size}, offsets: ${offsets.map(_.size).sumOrElse(0)}, expiry: ${expiry.map(_.size).sumOrElse(0)}"
  }

  private def newQueue = new PriorityBlockingQueue[(Long, Long, SimpleFeature)](1000, SharedState.OffsetComparator)
}

object SharedState {
  private val OffsetComparator = new Comparator[(Long, Long, SimpleFeature)]() {
    override def compare(o1: (Long, Long, SimpleFeature), o2: (Long, Long, SimpleFeature)): Int =
      java.lang.Long.compare(o1._1, o2._1)
  }
}