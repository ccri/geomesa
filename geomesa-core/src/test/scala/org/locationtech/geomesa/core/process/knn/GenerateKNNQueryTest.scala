/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.core.process.knn

import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.index.{Constants, FilterHelper}
import org.locationtech.geomesa.core.{filter, index}
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.filter.expression.Literal
import org.opengis.filter.spatial.BBOX
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class GenerateKNNQueryTest extends Specification {

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user" -> "myuser",
      "password" -> "mypassword",
      "auths" -> "A,B,C",
      "tableName" -> "testwrite",
      "useMock" -> "true",
      "featureEncoding" -> "avro").asJava).asInstanceOf[AccumuloDataStore]

  val sftName = "test"
  val sft = SimpleFeatureTypes.createType(sftName, index.spec)
  sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")

  val ds = createStore

  ds.createSchema(sft)

  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val smallGH = GeoHash("dqb0tg")

  val ff = CommonFactoryFinder.getFilterFactory2

  val WGS84 = DefaultGeographicCRS.WGS84


  "GenerateKNNQuery" should {
    " inject a small BBOX into a larger query and confirm that the original is untouched" in {
      val q =
        ff.and(
          ff.like(ff.property("prop"), "foo"),
          ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
        )

      // use the above to generate a Query
      val oldQuery = new Query(sftName, q)

      // and then generate a new one
      val newQuery = KNNQuery.generateKNNQuery(smallGH, oldQuery, fs)

      // check that oldQuery is untouched
      val oldFilter = oldQuery.getFilter

      // confirm that the oldFilter was not mutated by operations on the new filter
      // this confirms that the deep copy on the oldQuery was done properly
      oldFilter mustEqual q
    }

    "inject a small BBOX into a larger query and have the spatial predicate be equal to the GeoHash boundary" in {
      //define a loose BBOX
      val q =
        ff.and(
          ff.like(ff.property("prop"), "foo"),
          ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
        )

      // use the above to generate a Query
      val oldQuery = new Query(sftName, q)

      // and then generate a new one
      val newQuery = KNNQuery.generateKNNQuery(smallGH, oldQuery, fs)

      // get the newFilter
      val newFilter =  newQuery.getFilter

      // process the newFilter to split out the geometry part
      val (geomFilters, otherFilters) = filter.partitionGeom(newFilter, sft)

      // rewrite the geometry filter
      val tweakedGeoms = geomFilters.map ( FilterHelper.updateTopologicalFilters(_, sft) )

      //there can be only one
      tweakedGeoms.length mustEqual(1)

      val oneBBOX = tweakedGeoms.head.asInstanceOf[BBOX]

      val bboxPoly=oneBBOX.getExpression2.asInstanceOf[Literal].evaluate(null)

      // confirm that the extracted spatial predicate matches the GeoHash BBOX.
      bboxPoly.equals(smallGH.geom) must beTrue
    }
  }
}
