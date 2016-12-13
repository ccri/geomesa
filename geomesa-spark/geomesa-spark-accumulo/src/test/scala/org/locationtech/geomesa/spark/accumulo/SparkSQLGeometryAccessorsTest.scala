/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark.accumulo

import java.util.{Map => JMap}

import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometryAccessorsTest extends Specification {

  "sql geometry accessors" should {
    sequential

    System.setProperty(QueryProperties.SCAN_RANGES_TARGET.property, "1")
    System.setProperty(AccumuloQueryProperties.SCAN_BATCH_RANGES.property, s"${Int.MaxValue}")

    var mac: MiniAccumuloCluster = null
    var dsParams: JMap[String, String] = null
    var ds: AccumuloDataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null

    var df: DataFrame = null

    // before
    step {
      mac = SparkSQLTestUtils.setupMiniAccumulo()
      dsParams = SparkSQLTestUtils.createDataStoreParams(mac)
      ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

      spark = SparkSession.builder().master("local[*]").getOrCreate()
      sc = spark.sqlContext
      sc.setConf("spark.sql.crossJoin.enabled", "true")

      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      df.printSchema()
      df.createOrReplaceTempView("chicago")

      df.collect().length mustEqual 3
    }

    "st_boundary" >> {
      val result = sc.sql(
        """
          |select st_boundary(st_geomFromWKT('LINESTRING(1 1, 0 0, -1 1)'))
        """.stripMargin
      )
      result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("MULTIPOINT(1 1,-1 1)")
    }

    "st_coordDim" >> {
      val r = sc.sql(
        """
          |select st_makeBox2D(st_castToPoint(st_geomFromWKT('POINT(0 0)')),
          |                    st_castToPoint(st_geomFromWKT('POINT(2 2)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
                                                                 "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    "st_dimension" >> {
      success
    }

    "st_envelope" >> {
      success
    }

    "st_exteriorRing" >> {
      success
    }

    "st_geometryN" >> {
      success
    }

    "st_interiorRingN" >> {
      success
    }

    "st_isClosed" >> {
      success
    }

    "st_isCollection" >> {
      success
    }

    "st_isEmpty" >> {
      success
    }

    "st_isRing" >> {
      success
    }

    "st_isSimple" >> {
      success
    }

    "st_isValid" >> {
      success
    }

    "st_numGeometries" >> {
      success
    }

    "st_numPoints" >> {
      success
    }

    "st_pointN" >> {
      success
    }

    "st_x" >> {
      success
    }

    "st_y" >> {
      success
    }
  }
}
