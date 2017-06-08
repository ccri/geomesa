/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.geotools.data.DataStoreFinder
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils.decodeStat
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification

/**
  * Base class for running all hbase embedded tests
  */
@RunWith(classOf[JUnitRunner])
class HBaseTestRunnerTest extends Specification with LazyLogging {

  var cluster: HBaseTestingUtility = new HBaseTestingUtility()
  var connection: Connection = _

  // add new tests here
  val specs = Seq(
    new HBaseDataStoreTest,
    new HBaseVisibilityTest,
    new HBaseDensityFilterTest//,
    //new HBaseStatsAggregatorTest
  )

  step {
    logger.info("Starting embedded hbase")
    cluster.getConfiguration.set("hbase.superuser", "admin")
    cluster.getConfiguration.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      classOf[GeoMesaCoprocessor].getCanonicalName)
    cluster.startMiniCluster(1)
    connection = cluster.getConnection
    logger.info("Started embedded hbase")
    specs.foreach { s => s.cluster = cluster; s.connection = connection }
  }

  specs.foreach(link)

  step {
    logger.info("Stopping embedded hbase")
    // note: HBaseTestingUtility says don't close the connection
    // connection.close()
    cluster.shutdownMiniCluster()
    logger.info("Embedded HBase stopped")
  }
}

trait HBaseTest extends Specification {
  import scala.collection.JavaConversions._

  val TEST_FAMILY = "idt:java.lang.Integer:index=full,attr:java.lang.Long:index=join,dtg:Date,*geom:Point:srid=4326"
  val TEST_HINT = new Hints()
  val sftName = "test_sft"

  var typeName: String = _
  var cluster: HBaseTestingUtility = _
  var connection: Connection = _
  var sft: SimpleFeatureType = _
  var fs: SimpleFeatureStore = _

  lazy val params = Map(
    ConnectionParam.getName -> connection,
    BigTableNameParam.getName -> sftName)
  lazy val ds = DataStoreFinder.getDataStore(params).asInstanceOf[HBaseDataStore]

  step {
    ds.getSchema(typeName) must beNull
    ds.createSchema(SimpleFeatureTypes.createType(typeName, TEST_FAMILY))
    sft = ds.getSchema(typeName)
    fs = ds.getFeatureSource(typeName).asInstanceOf[SimpleFeatureStore]
  }

  def addFeatures(toAdd: Seq[SimpleFeature]) = {
    fs.addFeatures(new ListFeatureCollection(sft, toAdd))
  }
}
