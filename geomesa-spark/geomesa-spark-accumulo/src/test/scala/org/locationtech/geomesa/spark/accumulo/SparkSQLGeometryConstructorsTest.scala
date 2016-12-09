package org.locationtech.geomesa.spark.accumulo

import java.util.{Map => JMap}

import com.vividsolutions.jts.geom._
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometryConstructorsTest extends Specification {

  "sql geometry constructors" should {
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

    "st_geomFromGeoHash" >> {

      val hashString = GeoHash(0, 0).hash
      val r = sc.sql(
        s"""
           |select st_geomFromGeoHash('$hashString')
        """.stripMargin
      )

      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0 0, 0 0.0439453125, " +
        "0.0439453125 0.0439453125, 0.0439453125 0, 0 0))")
    }

    "st_geomFromWKT" >> {
      val r = sc.sql(
        """
          |select st_geomFromWKT('POINT(0 0)')
        """.stripMargin
      )

      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_geomFromWKB" >> {
      success
    }

    "st_makeBBOX" >> {
      val r = sc.sql(
        """
          |select st_makeBBOX(0.0, 0.0, 2.0, 2.0)
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    "st_makeBox2D" >> {
      val r = sc.sql(
        """
          |select st_makeBox2D(st_castToPoint(st_geomFromWKT('POINT(0 0)')),
          |                    st_castToPoint(st_geomFromWKT('POINT(2 2)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    "st_makePolygon" >> {
      val r = sc.sql(
        s"""
           |select st_makePolygon(st_castToLineString(
           |    st_geomFromWKT('LINESTRING(0.0 0.0, 2.0 0.0, 2.0 2.0, 0.0 2.0, 0.0 0.0)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    "st_makePoint" >> {
      val r = sc.sql(
        """
          |select st_makePoint(0, 0)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_makePointM" >> {
      val r = sc.sql(
        """
          |select st_makePointM(0, 0, 1)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0 1)")
    }

    "st_mLineFromText" >> {
      success
    }

    "st_mPointFromText" >> {
      val r = sc.sql(
        """
          |select st_mPointFromText('MULTIPOINT((0 0), (1 1))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiPoint](0) mustEqual WKTUtils.read("MULTIPOINT((0 0), (1 1))")
    }

    "st_mPolyFromText" >> {
      val r = sc.sql(
        """
          |select st_mPolyFromText('MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 )),((-4 4, 4 4, 4 -4, -4 -4, -4 4),
          |                                    (2 2, -2 2, -2 -2, 2 -2, 2 2)))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiPolygon](0) mustEqual
        WKTUtils.read("MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 ))," +
          "((-4 4, 4 4, 4 -4, -4 -4, -4 4),(2 2, -2 2, -2 -2, 2 -2, 2 2)))")
    }

    "st_point" >> {
      val r = sc.sql(
        """
          |select st_point(0, 0)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_pointFromGeoHash" >> {
      val hashString = GeoHash(0, 0).hash
      val r = sc.sql(
        s"""
           |select st_pointFromGeoHash('$hashString')
        """.stripMargin
      )

      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_pointFromText" >> {
      val r = sc.sql(
        """
          |select st_pointFromText('Point(0 0)')
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_pointFromWKB" >> {
      success
    }

    "st_polygon" >> {
      val r = sc.sql(
        s"""
           |select st_polygon(st_castToLineString(
           |    st_geomFromWKT('LINESTRING(0.0 0.0, 2.0 0.0, 2.0 2.0, 0.0 2.0, 0.0 0.0)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    "st_polygonFromText" >> {
      val r = sc.sql(
        """
          |select st_polygonFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    // after
    step {
      //mac.stop()
    }
  }
}
