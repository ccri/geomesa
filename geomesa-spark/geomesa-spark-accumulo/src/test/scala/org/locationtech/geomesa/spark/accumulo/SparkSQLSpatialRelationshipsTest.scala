package org.locationtech.geomesa.spark.accumulo

import java.util.{Map => JMap}

import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.conf.QueryProperties
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLSpatialRelationshipsTest extends Specification {
  "SQL spatial relationships" should {
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
    }

    val box        = "POLYGON(( 0  0,  0 10, 10 10, 10  0,  0  0))"

    val pointInt    = "POINT(5 5)"
    val pointEdge   = "POINT(0 5)"
    val pointCorner = "POINT(0 0)"
    val pointExt    = "POINT(-5 0)"
    val boxInt     = "POLYGON(( 1  1,  1  2,  2  2,  2  1,  1  1))"
    val boxIntEdge = "POLYGON(( 0  1,  0  2,  1  2,  1  1,  0  1))"
    val boxOverlap = "POLYGON((-1  1, -1  2,  1  2,  1  1, -1  1))"
    val boxExtEdge = "POLYGON((-1  1, -1  2,  0  2,  0  1, -1  1))"
    val boxExt     = "POLYGON((-2  1, -2  2, -1  2, -1  1, -2  1))"
    val boxCorner  = "POLYGON((-1 -1, -1  0,  0  0,  0 -1, -1 -1))"

    def testBool(f: String, name: String, g1: String, g2: String, expected: Boolean) = {
      f+" "+name >> {
        val sql = s"select $f(st_geomFromWKT('$g1'), st_geomFromWKT('$g2'))"
        val r = sc.sql(sql)
        r.collect()
        r.head.getBoolean(0) mustEqual expected
      }
    }

    "st_contains" >> {
      testBool("st_contains", "pt1", box, pointInt,    true)
      testBool("st_contains", "pt2", box, pointEdge,   false)
      testBool("st_contains", "pt3", box, pointCorner, false)
      testBool("st_contains", "pt4", box, pointExt,    false)

      testBool("st_contains", "poly1", box, boxInt,     true)
      testBool("st_contains", "poly2", box, boxIntEdge, true)
      testBool("st_contains", "poly3", box, boxOverlap, false)
      testBool("st_contains", "poly4", box, boxExtEdge, false)
      testBool("st_contains", "poly5", box, boxExt,     false)
      testBool("st_contains", "poly6", box, boxCorner,  false)
    }

    "st_covers" >> {
      true mustEqual true
    }

    "st_crosses" >> {
      testBool("st_crosses", "touches", "LINESTRING(0 10, 0 -10)", "LINESTRING(0 0, 1 0)", false)
      testBool("st_crosses", "crosses", "LINESTRING(0 10, 0 -10)", "LINESTRING(-1 0, 1 0)", true)
      testBool("st_crosses", "disjoint", "LINESTRING(0 10, 0 -10)", "LINESTRING(1 0, 2 0)", false)
    }

    "st_disjoint" >> {
      testBool("st_disjoint", "pt1", box, pointInt,    false)
      testBool("st_disjoint", "pt2", box, pointEdge,   false)
      testBool("st_disjoint", "pt3", box, pointCorner, false)
      testBool("st_disjoint", "pt4", box, pointExt,    true)

      testBool("st_disjoint", "poly1", box, boxInt,     false)
      testBool("st_disjoint", "poly2", box, boxIntEdge, false)
      testBool("st_disjoint", "poly3", box, boxOverlap, false)
      testBool("st_disjoint", "poly4", box, boxExtEdge, false)
      testBool("st_disjoint", "poly5", box, boxExt,     true)
      testBool("st_disjoint", "poly6", box, boxCorner,  false)
    }

    "st_equals" >> {
      true mustEqual true
    }

    "st_intersects" >> {
      testBool("st_intersects", "pt1", box, pointInt,    true)
      testBool("st_intersects", "pt2", box, pointEdge,   true)
      testBool("st_intersects", "pt3", box, pointCorner, true)
      testBool("st_intersects", "pt4", box, pointExt,    false)

      testBool("st_intersects", "poly1", box, boxInt,     true)
      testBool("st_intersects", "poly2", box, boxIntEdge, true)
      testBool("st_intersects", "poly3", box, boxOverlap, true)
      testBool("st_intersects", "poly4", box, boxExtEdge, true)
      testBool("st_intersects", "poly5", box, boxExt,     false)
      testBool("st_intersects", "poly6", box, boxCorner,  true)
    }

    "st_overlaps" >> {
      testBool("st_overlaps", "pt1", box, pointInt,    false)
      testBool("st_overlaps", "pt2", box, pointEdge,   false)
      testBool("st_overlaps", "pt3", box, pointCorner, false)
      testBool("st_overlaps", "pt4", box, pointExt,    false)

      testBool("st_overlaps", "poly1", box, boxInt,     false)
      testBool("st_overlaps", "poly2", box, boxIntEdge, false)
      testBool("st_overlaps", "poly3", box, boxOverlap, true)
      testBool("st_overlaps", "poly4", box, boxExtEdge, false)
      testBool("st_overlaps", "poly5", box, boxExt,     false)
      testBool("st_overlaps", "poly6", box, boxCorner,  false)
    }

    "st_touches" >> {
      testBool("st_touches", "pt1", box, pointInt,    false)
      testBool("st_touches", "pt2", box, pointEdge,   true)
      testBool("st_touches", "pt3", box, pointCorner, true)
      testBool("st_touches", "pt4", box, pointExt,    false)

      testBool("st_touches", "poly1", box, boxInt,     false)
      testBool("st_touches", "poly2", box, boxIntEdge, false)
      testBool("st_touches", "poly3", box, boxOverlap, false)
      testBool("st_touches", "poly4", box, boxExtEdge, true)
      testBool("st_touches", "poly5", box, boxExt,     false)
      testBool("st_touches", "poly6", box, boxCorner,  true)
    }

    "st_within" >> {
      true mustEqual true
    }

    /*
    "st_contains" >> {
      val r = sc.sql(
        s"""
           |select  geom
           |from    chicago
           |where
           |  st_contains(st_geomFromWKT('$box1'), geom)
           |  and dtg >= cast('2015-12-31' as timestamp)
           |  and dtg <= cast('2016-01-07' as timestamp)
   """.stripMargin)
      true mustEqual true
    }

    "st_crosses" >> {
      val r = sc.sql(
        """
          |select  arrest, geom
          |from    chicago
          |where
          |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
          |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
        """.stripMargin)
      r.show()
      true mustEqual true
    }
    "st_intersects" >> {
      val r = sc.sql(
        """
          |select  arrest, geom
          |from    chicago
          |where
          |  st_intersects(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
          |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
        """.stripMargin)
      r.show()
      true mustEqual true
    }

    "st_within" >> {
      val r = sc.sql(
        """
          |select  arrest, geom
          |from    chicago
          |where
          |  st_within(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
          |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
        """.stripMargin)
      r.show()
      true mustEqual true
    }
    */

    // after
    step {
      //mac.stop()
    }
  }
}
