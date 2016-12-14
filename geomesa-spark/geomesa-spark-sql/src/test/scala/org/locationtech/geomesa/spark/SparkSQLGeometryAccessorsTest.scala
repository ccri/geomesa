/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Polygon}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometryAccessorsTest extends Specification with LazyLogging {

  "sql geometry accessors" should {
    sequential

    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")
    var ds: DataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null

    var df: DataFrame = null

    // before
    step {
      ds = DataStoreFinder.getDataStore(dsParams)
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext

      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      logger.info(df.schema.treeString)
      df.createOrReplaceTempView("chicago")

      df.collect().length mustEqual 3
    }

    "st_boundary" >> {
      val result = sc.sql(
        """
          |select st_boundary(st_geomFromWKT('LINESTRING(1 1, 0 0, -1 1)'))
        """.stripMargin
      )
      result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("MULTIPOINT(1 1, -1 1)")
    }

    "st_coordDim" >> {
      val result = sc.sql(
        """
          |select st_coordDim(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 2
    }

    "st_dimension" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 0
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('LINESTRING(1 1, 0 0, -1 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
      }

      "polygon" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 2
      }

      // TODO: uncomment when GeometryCollection is supported
//      "geometrycollection" >> {
//        val result = sc.sql(
//          """
//            |select st_dimension(st_geomFromWKT('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))'))
//          """.stripMargin
//        )
//        result.collect().head.getAs[Int](0) mustEqual 1
//      }
    }

    "st_envelope" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('LINESTRING(0 0, 1 3)'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0 0,0 3,1 3,1 0,0 0))")
      }

      "polygon" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))")
      }
    }

    "st_exteriorRing" >> {
      success
    }

    "st_geometryN" >> {
      "point" >> {
        val result = sc.sql(
          """
            |select st_geometryN(st_geomFromWKT('POINT(0 0)'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
      }

      "multilinestring" >> {
        val result = sc.sql(
          """
            |select st_geometryN(st_geomFromWKT('MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("LINESTRING(40 40, 30 30, 40 20, 30 10)")
      }

      // TODO: uncomment when GeometryCollection is supported
//      "geometrycollection" >> {
//        val result = sc.sql(
//          """
//            |select st_dimension(st_geomFromWKT('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))'), 1)
//          """.stripMargin
//        )
//        result.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
//      }
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

    // after
    step {
    }
  }
}
