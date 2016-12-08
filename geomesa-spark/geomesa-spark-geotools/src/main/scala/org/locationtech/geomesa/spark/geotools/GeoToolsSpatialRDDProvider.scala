/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.geotools

import java.io.Serializable
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.geotools.data.{DataStoreFinder, Query, Transaction}
import org.locationtech.geomesa.spark.SpatialRDDProvider
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

class GeoToolsSpatialRDDProvider extends SpatialRDDProvider {
  override def canProcess(params: util.Map[String, Serializable]): Boolean = {
    params.containsKey("geotools") && DataStoreFinder.getDataStore(params) != null
  }

  override def rdd(conf: Configuration, sc: SparkContext, dsParams: Map[String, String], query: Query, numberOfSplits: Option[Int]): RDD[SimpleFeature] = {
    val ds = DataStoreFinder.getDataStore(dsParams)
    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)

    sc.parallelize(fr.toIterator.toSeq)
  }

  /**
    * Writes this RDD to a GeoMesa table.
    * The type must exist in the data store, and all of the features in the RDD must be of this type.
    *
    * @param rdd
    * @param writeDataStoreParams
    * @param writeTypeName
    */
  override def save(rdd: RDD[SimpleFeature], writeDataStoreParams: Map[String, String], writeTypeName: String): Unit = {
    val ds = DataStoreFinder.getDataStore(writeDataStoreParams)

    try {
      require(ds.getSchema(writeTypeName) != null,
        "Feature type must exist before calling save.  Call .createSchema on the DataStore before calling .save")
    } finally {
      ds.dispose()
    }

    rdd.foreachPartition { iter =>
      val ds = DataStoreFinder.getDataStore(writeDataStoreParams)
      val featureWriter = ds.getFeatureWriterAppend(writeTypeName, Transaction.AUTO_COMMIT)
      val attrNames = featureWriter.getFeatureType.getAttributeDescriptors.map(_.getLocalName)
      try {
        iter.foreach { case rawFeature =>
          val newFeature = featureWriter.next()
          attrNames.foreach(an => newFeature.setAttribute(an, rawFeature.getAttribute(an)))
          featureWriter.write()
        }
      } finally {
        featureWriter.close()
        ds.dispose()
      }
    }



  }
}
