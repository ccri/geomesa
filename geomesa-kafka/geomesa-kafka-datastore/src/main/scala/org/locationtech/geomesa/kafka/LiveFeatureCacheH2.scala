/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import org.geotools.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.utils.geotools.FR
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConversions._

/**
  * EXPERIMENTAL!
  */
// NB: The necessary dependencies have been commented out...
class LiveFeatureCacheH2(sft: SimpleFeatureType) extends LiveFeatureCache {
  val ff = CommonFactoryFinder.getFilterFactory2
  val params = Map("dbtype" -> "h2gis", "database" -> "mem:db1")
  val ds = DataStoreFinder.getDataStore(params)
  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sft.getTypeName).asInstanceOf[SimpleFeatureStore]
  val attrNames = sft.getAttributeDescriptors.map(_.getLocalName).toArray

  /*
  val h2_pop = timeUnit({
    val fc = new DefaultFeatureCollection(sft.getTypeName, sft)
    fc.addAll(feats)
    fs.addFeatures(fc)
  })
  */

  override def cleanUp(): Unit = { /* vacuum? */ }

  override def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val filter = ff.id(sf.getIdentifier)
    if (fs.getFeatures(filter).size > 0) {
      val attrValues = attrNames.toList.map(sf.getAttribute(_)).toArray
      fs.modifyFeatures(attrNames, attrValues, filter)
    }
    else {
      val fc = new DefaultFeatureCollection(sft.getTypeName, sft)
      fc.add(sf)
      fs.addFeatures(fc)
    }
  }

  override def getFeatureById(id: String): FeatureHolder = ???

  override def removeFeature(toDelete: Delete): Unit = ???

  override def clear(): Unit = ???

  override def size(): Int = ???

  override def size(filter: Filter): Int = ???

  override def getReaderForFilter(filter: Filter): FR = ???

  def getFeatures(filter: Filter) = fs.getFeatures(filter)
}
