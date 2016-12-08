/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.datastore

import java.util.Date

import com.googlecode.cqengine.index.hash.HashIndex
import com.googlecode.cqengine.index.navigable.NavigableIndex
import com.googlecode.cqengine.index.radix.RadixTreeIndex
import com.googlecode.cqengine.index.support.AbstractAttributeIndex
import com.googlecode.cqengine.index.unique.UniqueIndex
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.{DataStoreFinder, DataUtilities, FeatureStore, Query}
import org.geotools.feature.FeatureCollection
import org.junit.runner.RunWith
import org.locationtech.geomesa.memory.cqengine.{GeoCQEngine, utils}
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexingOptions._
import org.locationtech.geomesa.memory.cqengine.utils.SampleFeatures
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoCQEngineDataStoreTest extends Specification {

  sequential

  private val feats = (0 until 1000).map(SampleFeatures.buildFeature)

  "GeoCQEngineData" should {

    val params = Map("cqengine" -> "true")
    val ds = DataStoreFinder.getDataStore(params)

    "get a datastore" in {
      ds mustNotEqual null
    }

    "createSchema" in {
      ds.createSchema(SampleFeatures.sft)
      ds.getTypeNames.length mustEqual 1
      ds.getTypeNames.contains("test") mustEqual true
    }

    "insert features" in {
      val fs = ds.getFeatureSource("test").asInstanceOf[GeoCQEngineFeatureStore]

      val fw = fs.getWriter(Query.ALL)
      fs.addFeatures(DataUtilities.collection(feats))

      fs mustNotEqual null
      fs.getCount(Query.ALL) mustEqual 1000
    }
  }
}
