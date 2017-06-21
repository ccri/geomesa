/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStoreFinder, DataUtilities, Query, Transaction}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.lambda.LambdaTestRunnerTest.LambdaTest
import org.locationtech.geomesa.lambda.data.LambdaDataStore
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.{FeatureUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.stats.{EnumerationStat, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult

class LambdaDataStoreTest extends LambdaTest with LazyLogging {

  import scala.collection.JavaConversions._

  sequential

  step {
    logger.info("LambdaDataStoreTest starting")
  }

  val sft = SimpleFeatureTypes.createType("lambda", "name:String,dtg:Date,*geom:Point:srid=4326")
  val features = Seq(
    ScalaSimpleFeature.create(sft, "0", "n0", "2017-06-15T00:00:00.000Z", "POINT (45 50)"),
    ScalaSimpleFeature.create(sft, "1", "n1", "2017-06-15T00:00:01.000Z", "POINT (46 51)")
  )

  def testTransforms(ds: LambdaDataStore, transform: SimpleFeatureType): MatchResult[Any] = {
    val query = new Query(sft.getTypeName, Filter.INCLUDE, transform.getAttributeDescriptors.map(_.getLocalName).toArray)
    // note: need to copy the features as the same object is re-used in the iterator
    val iter = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
    val result = iter.map(DataUtilities.encodeFeature).toSeq
    val expected = features.map(DataUtilities.reType(transform, _)).map(DataUtilities.encodeFeature)
    result must containTheSameElementsAs(expected)
  }

  def testBin(ds: LambdaDataStore): MatchResult[Any] = {
    val query = new Query(sft.getTypeName, Filter.INCLUDE)
    query.getHints.put(QueryHints.BIN_TRACK, "name")
    // note: need to copy the features as the same object is re-used in the iterator
    val iter = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
    val bytes = iter.map(_.getAttribute(0).asInstanceOf[Array[Byte]]).reduceLeftOption(_ ++ _).getOrElse(Array.empty[Byte])
    bytes must haveLength(32)
    val bins = bytes.grouped(16).map(Convert2ViewerFunction.decode).toSeq
    bins.map(_.trackId) must containAllOf(Seq("n0", "n1").map(_.hashCode))
    bins.map(_.dtg) must containAllOf(features.map(_.getAttribute("dtg").asInstanceOf[Date].getTime))
    bins.map(_.lat) must containAllOf(Seq(50f, 51f))
    bins.map(_.lon) must containAllOf(Seq(45f, 46f))
  }

  def testStats(ds: LambdaDataStore): MatchResult[Any] = {
    val query = new Query(sft.getTypeName, Filter.INCLUDE)
    query.getHints.put(QueryHints.STATS_STRING, Stat.Enumeration("name"))
    query.getHints.put(QueryHints.ENCODE_STATS, true)

    // note: need to copy the features as the same object is re-used in the iterator
    val result = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT)).toSeq
    result must haveLength(1)
    val stat = KryoLazyStatsUtils.decodeStat(sft)(result.head.getAttribute(0).asInstanceOf[String])
    stat must beAnInstanceOf[EnumerationStat[String]]
    stat.asInstanceOf[EnumerationStat[String]].frequencies must containTheSameElementsAs(Seq(("n0", 1L), ("n1", 1L)))

    val jsonQuery = new Query(sft.getTypeName, Filter.INCLUDE)
    jsonQuery.getHints.put(QueryHints.STATS_STRING, Stat.Enumeration("name"))
    jsonQuery.getHints.put(QueryHints.ENCODE_STATS, false)
    val jsonResult = SelfClosingIterator(ds.getFeatureReader(jsonQuery, Transaction.AUTO_COMMIT)).toSeq
    jsonResult must haveLength(1)
    jsonResult.head.getAttribute(0).asInstanceOf[String] must (contain(""""n0":1""") and contain(""""n1":1"""))
    jsonResult.head.getAttribute(0).asInstanceOf[String] must haveLength(15)
  }

  "LambdaDataStore" should {
    "write and read features" in {
      val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[LambdaDataStore]
      ds must not(beNull)
      try {
        ds.createSchema(sft)
        ds.getSchema(sft.getTypeName) mustEqual sft
        WithClose(ds.getFeatureWriterAppend(sft.getTypeName, Transaction.AUTO_COMMIT)) { writer =>
          features.foreach { feature =>
            FeatureUtils.copyToWriter(writer, feature, useProvidedFid = true)
            writer.write()
            clock.tick = clock.millis + 50
          }
        }
        // test queries against the transient store
        ds.transients.get(sft.getTypeName).read().toSeq must eventually(40, 100.millis)(containTheSameElementsAs(features))
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features)
        testTransforms(ds, SimpleFeatureTypes.createType("lambda", "*geom:Point:srid=4326"))
        testBin(ds)
        testStats(ds)

        // persist one feature to long-term store
        clock.tick = 101
        ds.persist(sft.getTypeName)
        ds.transients.get(sft.getTypeName).read().toSeq must eventually(40, 100.millis)(beEqualTo(features.drop(1)))
        // test mixed queries against both stores
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features)
        testTransforms(ds, SimpleFeatureTypes.createType("lambda", "*geom:Point:srid=4326"))
        testBin(ds)
        testStats(ds)

        // persist both features to the long-term storage
        clock.tick = 151
        ds.persist(sft.getTypeName)
        ds.transients.get(sft.getTypeName).read() must eventually(40, 100.millis)(beEmpty)
        // test queries against the persistent store
        SelfClosingIterator(ds.getFeatureReader(new Query(sft.getTypeName), Transaction.AUTO_COMMIT)).toSeq must
            containTheSameElementsAs(features)
        testTransforms(ds, SimpleFeatureTypes.createType("lambda", "*geom:Point:srid=4326"))
        testBin(ds)
        testStats(ds)
      } finally {
        ds.dispose()
      }
//      val typeName = "testpoints"
//
//      val params = Map(ConnectionParam.getName -> connection, BigTableNameParam.getName -> catalogTableName)
//      val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]
//
//      ds.getSchema(typeName) must beNull
//
//      ds.createSchema(SimpleFeatureTypes.createType(typeName, "name:String:index=true,attr:String,dtg:Date,*geom:Point:srid=4326"))
//
//      val sft = ds.getSchema(typeName)
//
//      sft must not(beNull)
//
//      val fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
//
//      val toAdd = (0 until 10).map { i =>
//        val sf = new ScalaSimpleFeature(i.toString, sft)
//        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
//        sf.setAttribute(0, s"name$i")
//        sf.setAttribute(1, s"name$i")
//        sf.setAttribute(2, f"2014-01-${i + 1}%02dT00:00:01.000Z")
//        sf.setAttribute(3, s"POINT(4$i 5$i)")
//        sf
//      }
//
//      val ids = fs.addFeatures(new ListFeatureCollection(sft, toAdd))
//      ids.asScala.map(_.getID) must containTheSameElementsAs((0 until 10).map(_.toString))
//
//      val transformsList = Seq(null, Array("geom"), Array("geom", "dtg"), Array("geom", "name"))
//
//      foreach(Seq(true, false)) { remote =>
//        foreach(Seq(true, false)) { loose =>
//          val settings = Map(LooseBBoxParam.getName -> loose, RemoteFiltersParam.getName -> remote)
//          val ds = DataStoreFinder.getDataStore(params ++ settings).asInstanceOf[HBaseDataStore]
//          foreach(transformsList) { transforms =>
//            testQuery(ds, typeName, "INCLUDE", transforms, toAdd)
//            testQuery(ds, typeName, "IN('0', '2')", transforms, Seq(toAdd(0), toAdd(2)))
//            testQuery(ds, typeName, "bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, toAdd.dropRight(2))
//            testQuery(ds, typeName, "bbox(geom,42,48,52,62)", transforms, toAdd.drop(2))
//            testQuery(ds, typeName, "attr = 'name5' and bbox(geom,38,48,52,62) and dtg DURING 2014-01-01T00:00:00.000Z/2014-01-08T12:00:00.000Z", transforms, Seq(toAdd(5)))
//            testQuery(ds, typeName, "name < 'name5'", transforms, toAdd.take(5))
//            testQuery(ds, typeName, "name = 'name5'", transforms, Seq(toAdd(5)))
//          }
//        }
//      }
//
//      def testTransforms(ds: HBaseDataStore) = {
//        val transforms = Array("derived=strConcat('hello',name)", "geom")
//        forall(Seq(("INCLUDE", toAdd), ("bbox(geom,42,48,52,62)", toAdd.drop(2)))) { case (filter, results) =>
//          val fr = ds.getFeatureReader(new Query(typeName, ECQL.toFilter(filter), transforms), Transaction.AUTO_COMMIT)
//          val features = SelfClosingIterator(fr).toList
//          features.headOption.map(f => SimpleFeatureTypes.encodeType(f.getFeatureType)) must
//            beSome("*geom:Point:srid=4326,derived:String")
//          features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
//          forall(features) { feature =>
//            feature.getAttribute("derived") mustEqual s"helloname${feature.getID}"
//            feature.getAttribute("geom") mustEqual results.find(_.getID == feature.getID).get.getAttribute("geom")
//          }
//        }
//      }
//
//      testTransforms(ds)
      ok
    }
  }

//  def testQuery(ds: HBaseDataStore, typeName: String, filter: String, transforms: Array[String], results: Seq[SimpleFeature]): MatchResult[Any] = {
//    val query = new Query(typeName, ECQL.toFilter(filter), transforms)
//    val fr = ds.getFeatureReader(query, Transaction.AUTO_COMMIT)
//    val features = SelfClosingIterator(fr).toList
//    val attributes = Option(transforms).getOrElse(ds.getSchema(typeName).getAttributeDescriptors.map(_.getLocalName).toArray)
//    features.map(_.getID) must containTheSameElementsAs(results.map(_.getID))
//    forall(features) { feature =>
//      feature.getAttributes must haveLength(attributes.length)
//      forall(attributes.zipWithIndex) { case (attribute, i) =>
//        feature.getAttribute(attribute) mustEqual feature.getAttribute(i)
//        feature.getAttribute(attribute) mustEqual results.find(_.getID == feature.getID).get.getAttribute(attribute)
//      }
//    }
//    ds.getFeatureSource(typeName).getFeatures(query).size() mustEqual results.length
//  }

  step {
    logger.info("LambdaDataStoreTest complete")
  }
}
