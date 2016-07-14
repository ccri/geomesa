/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.filter

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.filter.TestFilters._
import org.locationtech.geomesa.accumulo.iterators.TestData
import org.locationtech.geomesa.accumulo.{TestWithDataStore, TestWithMultipleSfts}
import org.locationtech.geomesa.accumulo.util.SelfClosingIterator
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterTest extends Specification with TestWithMultipleSfts with LazyLogging {

  lazy val defaultSft = createNewSchema(SimpleFeatureTypes.encodeType(TestData.featureType, true))
  lazy val defaultTypeName = defaultSft.getTypeName

  lazy val mediumDataFeatures: Seq[SimpleFeature] =
    TestData.mediumData.map(TestData.createSF).map(f => new ScalaSimpleFeature(f.getID, defaultSft, f.getAttributes.toArray))

  "Filters" should {

    addFeatures(defaultSft, mediumDataFeatures)

    "filter correctly for all predicates" >> {
      runTest(goodSpatialPredicates)
    }

    "filter correctly for AND geom predicates" >> {
      runTest(andedSpatialPredicates)
    }

    "filter correctly for OR geom predicates" >> {
      runTest(oredSpatialPredicates)
    }

    "filter correctly for OR geom predicates with projections" >> {
      runTest(oredSpatialPredicates, projection = Array("geom"))
    }

    "filter correctly for basic temporal predicates" >> {
      runTest(temporalPredicates)
    }

    "filter correctly for basic spatiotemporal predicates" >> {
      runTest(spatioTemporalPredicates)
    }

    "filter correctly for basic spatiotemporal predicates with namespaces" >> {
      runTest(spatioTemporalPredicatesWithNS)
    }

    "filter correctly for attribute predicates" >> {
      runTest(attributePredicates)
    }

    "filter correctly for attribute and geometric predicates" >> {
      runTest(attributeAndGeometricPredicates)
    }

    "filter correctly for attribute and geometric predicates with namespaces" >> {
      runTest(attributeAndGeometricPredicatesWithNS)
    }

//    "filter correctly for DWITHIN predicates" >> {
//      runTest(dwithinPointPredicates)
//    }

    "filter correctly for ID predicates" >> {
      runTest(idPredicates)
    }

    "filter correctly for CONTAINS filters against schemas with MultiPolygon geoms" >> {
      val sft = createNewSchema("geom:MultiPolygon:srid=4326,dtg:Date")
      val sftName = sft.getTypeName
      val feature = new ScalaSimpleFeature("1", sft)
      // this multiPolygon is a BBOX within a BBOX
      val multiPolygon = "MULTIPOLYGON(((0 50, 0 20, 30 20, 30 50, 0 50), (10 40, 10 30, 20 30, 20 40, 10 40)))"

      feature.setAttribute(0, WKTUtils.read(multiPolygon))
      feature.setAttribute(1, "2014-01-01T00:00:00.000Z")

      addFeature(sft, feature)

      val filters = Seq(
        "CONTAINS(geom, POINT (-5 35))",
        "CONTAINS(geom, POINT (5 35))",
        "CONTAINS(geom, POINT (15 35))",
        "CONTAINS(geom, POINT (25 35))",
        "CONTAINS(geom, POINT (35 35))",
        "CONTAINS(geom, LINESTRING (-5 35, 5 35))",
        "CONTAINS(geom, LINESTRING (5 30, 5 40))",
        "CONTAINS(geom, LINESTRING (5 35, 25 35))"
      )

      runTest(filters, sftName, Seq(feature))
    }
  }

  def compareFilter(filter: Filter,
                    fs: SimpleFeatureSource,
                    dataFeatures: Seq[SimpleFeature],
                    projection: Array[String]) = {

    val filterCount = dataFeatures.count(filter.evaluate)
    val query = new Query(defaultTypeName, filter)
    Option(projection).foreach(query.setPropertyNames)
    val queryCount = SelfClosingIterator(fs.getFeatures(query)).length

    val logStatement = s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${dataFeatures.size}: " +
      s"filter hits: $filterCount query hits: $queryCount"
    if (filterCount != queryCount) {
      logger.error(logStatement)
    } else {
      logger.debug(logStatement)
    }

    (queryCount, filterCount)
  }

  def runTest(filters: Seq[String],
              sftName: String = defaultTypeName,
              dataFeatures: Seq[SimpleFeature] = mediumDataFeatures,
              projection: Array[String] = null) = {

    val fs = ds.getFeatureSource(sftName)
    val (queryCounts, filterCounts) = filters.map(ECQL.toFilter).map(compareFilter(_, fs, dataFeatures, projection)).unzip
    queryCounts mustEqual filterCounts
  }
}

@RunWith(classOf[JUnitRunner])
class IdQueryTest extends Specification with TestWithDataStore {

  override val spec = "age:Int:index=true,name:String:index=true,dtg:Date,*geom:Point:srid=4326"

  val ff = CommonFactoryFinder.getFilterFactory2
  val geomBuilder = JTSFactoryFinder.getGeometryFactory
  val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
  val data = List(
    ("1", Array(10, "johndoe", new Date), geomBuilder.createPoint(new Coordinate(10, 10))),
    ("2", Array(20, "janedoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20))),
    ("3", Array(30, "johnrdoe", new Date), geomBuilder.createPoint(new Coordinate(20, 20)))
  )
  val features = data.map { case (id, attrs, geom) =>
    builder.reset()
    builder.addAll(attrs.asInstanceOf[Array[AnyRef]])
    val f = builder.buildFeature(id)
    f.setDefaultGeometry(geom)
    f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
    f
  }

  addFeatures(features)

  "Id queries" should {
    "use record table to return a result" >> {
      val idQ = ff.id(ff.featureId("2"))
      val res = fs.getFeatures(idQ).features().toList
      res.length mustEqual 1
      res.head.getID mustEqual "2"
    }

    "handle multiple ids correctly" >> {
      val idQ = ff.id(ff.featureId("1"), ff.featureId("3"))
      val res = fs.getFeatures(idQ).features().toList
      res.length mustEqual 2
      res.map(_.getID) must contain ("1", "3")
    }

    "return no events when multiple IDs ANDed result in no intersection"  >> {
      val idQ1 = ff.id(ff.featureId("1"), ff.featureId("3"))
      val idQ2 = ff.id(ff.featureId("2"))
      val idQ =  ff.and(idQ1, idQ2)
      val qRes = fs.getFeatures(idQ)
      val res= qRes.features().toList
      res.length mustEqual 0
    }
  }
}
