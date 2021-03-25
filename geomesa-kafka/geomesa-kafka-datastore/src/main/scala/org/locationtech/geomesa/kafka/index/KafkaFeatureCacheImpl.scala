/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.util
import java.util.concurrent._

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import org.locationtech.geomesa.filter.index.{BucketIndexSupport, SizeSeparatedBucketIndexSupport, SpatialIndexSupport}
import org.locationtech.geomesa.kafka.data.KafkaDataStore.IndexConfig
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.{FeatureExpiration, FeatureState}
import org.locationtech.geomesa.utils.concurrent.ExitingExecutor
import org.locationtech.geomesa.utils.io.Sizable
import org.locationtech.geomesa.utils.io.Sizable.{deepSizeOf, sizeOf}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConversions.asScalaSet

/**
  * Feature cache implementation
  *
  * @param sft simple feature type
  * @param config index config
  */
class KafkaFeatureCacheImpl(val sft: SimpleFeatureType, config: IndexConfig)
    extends KafkaFeatureCache with FeatureExpiration with StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // keeps location and expiry keyed by feature ID (we need a way to retrieve a feature based on ID for
  // update/delete operations). to reduce contention, we never iterate over this map
  private val state = new ConcurrentHashMap[String, FeatureState]

  // note: CQEngine handles points vs non-points internally
  private val support: SpatialIndexSupport = if (config.cqAttributes.nonEmpty) {
    KafkaFeatureCache.cqIndexSupport(sft, config)
  } else if (sft.isPoints) {
    BucketIndexSupport(sft, config.resolution.x, config.resolution.y)
  } else {
    SizeSeparatedBucketIndexSupport(sft, config.ssiTiers, config.resolution.x / 360d, config.resolution.y / 180d)
  }

  private val factory = FeatureStateFactory(sft, support.index, config.expiry, this, config.executor)

  KafkaFeatureCacheImpl.add(this)

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def put(feature: SimpleFeature): Unit = {
    val featureState = factory.createState(feature)
    logger.trace(s"${featureState.id} adding feature $featureState")
    val old = state.put(featureState.id, featureState)
    if (old == null) {
      featureState.insertIntoIndex()
    } else if (old.time <= featureState.time) {
      logger.trace(s"${featureState.id} removing old feature")
      old.removeFromIndex()
      featureState.insertIntoIndex()
    } else {
      logger.trace(s"${featureState.id} ignoring out of sequence feature")
      if (!state.replace(featureState.id, featureState, old)) {
        logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
        old.removeFromIndex()
      }
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def remove(id: String): Unit = {
    logger.trace(s"$id removing feature")
    val old = state.remove(id)
    if (old != null) {
      old.removeFromIndex()
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  override def expire(featureState: FeatureState): Unit = {
    logger.trace(s"${featureState.id} expiring from index")
    if (state.remove(featureState.id, featureState)) {
      featureState.removeFromIndex()
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  override def clear(): Unit = {
    logger.trace("Clearing index")
    state.clear()
    support.index.clear()
  }

  override def size(): Int = state.size()

  // optimized for filter.include
  override def size(f: Filter): Int = if (f == Filter.INCLUDE) { size() } else { query(f).length }

  override def query(id: String): Option[SimpleFeature] =
    Option(state.get(id)).flatMap(f => Option(f.retrieveFromIndex()))

  override def query(filter: Filter): Iterator[SimpleFeature] = support.query(filter)

  override def close(): Unit = {
    factory.close()
    KafkaFeatureCacheImpl.remove(this)
  }
}


object KafkaFeatureCacheImpl extends LazyLogging {
  val caches = new util.HashSet[KafkaFeatureCacheImpl]

  def add(cache: KafkaFeatureCacheImpl) = caches.add(cache)
  def remove(cache: KafkaFeatureCacheImpl) = caches.remove(cache)

  val executor = ExitingExecutor(Executors.newScheduledThreadPool(2).asInstanceOf[ScheduledThreadPoolExecutor])

  executor.scheduleAtFixedRate(runnable, 60, 60, TimeUnit.SECONDS)

  def runnable = new Runnable {
    override def run(): Unit = {
      caches.foreach { c =>
        val start = System.currentTimeMillis()
        var count = 0L
        var sizeInMemory = 0L

        c.support.index.query().foreach { f =>
          val recordSize = f match {
            case s: Sizable => s.calculateSizeOf()
            case _ => deepSizeOf(f)
          }
          count += 1
          sizeInMemory += recordSize
        }
        val midpoint = System.currentTimeMillis()

        val deepSize = deepSizeOf(c)

        logger.debug(s"Type ${c.sft.getTypeName} has ${count} records.  Sizable: $sizeInMemory took ${midpoint-start}.  Deep size of cache: $deepSize took ${System.currentTimeMillis() - midpoint}")
      }
    }
  }
}