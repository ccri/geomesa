/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.util.concurrent.TimeUnit

import com.google.common.base.Ticker
import com.google.common.cache._
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.index.{BucketIndex, SpatialIndex}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by mzimmerman on 8/27/16.
  */
/** @param sft the [[SimpleFeatureType]]
  * @param expirationPeriod the number of milliseconds after write to expire a feature or ``None`` to not
  *                         expire
  * @param ticker used to determine elapsed time for expiring entries
  */
class LiveFeatureCacheGuava(override val sft: SimpleFeatureType,
                            expirationPeriod: Option[Long])(implicit ticker: Ticker)
  extends KafkaConsumerFeatureCache with LiveFeatureCache with LazyLogging {

  var spatialIndex: SpatialIndex[SimpleFeature] = newSpatialIndex()

  private val cache: Cache[String, FeatureHolder] = {
    val cb = CacheBuilder.newBuilder().ticker(ticker)
    expirationPeriod.foreach { ep =>
      cb.expireAfterWrite(ep, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener[String, FeatureHolder] {
          def onRemoval(removal: RemovalNotification[String, FeatureHolder]) = {
            if (removal.getCause == RemovalCause.EXPIRED) {
              logger.debug(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms")
              spatialIndex.remove(removal.getValue.env, removal.getValue.sf)
            }
          }
        })
    }
    cb.build()
  }

  override val features: mutable.Map[String, FeatureHolder] = cache.asMap().asScala

  override def cleanUp(): Unit = cache.cleanUp()

  override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = sf.getID
    val old = cache.getIfPresent(id)
    if (old != null) {
      spatialIndex.remove(old.env, old.sf)
    }
    val env = sf.geometry.getEnvelopeInternal
    spatialIndex.insert(env, sf)
    cache.put(id, FeatureHolder(sf, env))
  }

  override def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    val old = cache.getIfPresent(id)
    if (old != null) {
      spatialIndex.remove(old.env, old.sf)
      cache.invalidate(id)
    }
  }

  override def clear(): Unit = {
    cache.invalidateAll()
    spatialIndex = newSpatialIndex()
  }

  override def getFeatureById(id: String): FeatureHolder = cache.getIfPresent(id)

  private def newSpatialIndex() = new BucketIndex[SimpleFeature]
}
