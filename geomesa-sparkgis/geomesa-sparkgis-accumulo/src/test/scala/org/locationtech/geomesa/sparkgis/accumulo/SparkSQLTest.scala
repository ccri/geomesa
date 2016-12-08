package org.locationtech.geomesa.sparkgis.accumulo

import java.util.{Map => JMap}
import javafx.geometry.BoundingBox

import com.vividsolutions.jts.geom._
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.DataStoreFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLTest extends Specification {
  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  "SparkSQL" should {
    sequential

    System.setProperty(QueryProperties.SCAN_RANGES_TARGET.property, "1")
    System.setProperty(AccumuloQueryProperties.SCAN_BATCH_RANGES.property, s"${Int.MaxValue}")
    System.setProperty("sun.net.spi.nameservice.nameservers", "192.168.2.77")
    System.setProperty("sun.net.spi.nameservice.provider.1", "dns,sun")

    var mac: MiniAccumuloCluster = null
    var dsParams: JMap[String, String] = null
    var ds: AccumuloDataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null

    var df: DataFrame = null
    var gndf: DataFrame = null
    var sdf: DataFrame = null

    "init" >> {
      mac = SparkSQLTestUtils.setupMiniAccumulo()
      dsParams = SparkSQLTestUtils.createDataStoreParams(mac)
      println(dsParams)
      ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

      spark = SparkSession.builder().master("local[*]").getOrCreate()
      sc = spark.sqlContext
      sc.setConf("spark.sql.crossJoin.enabled", "true")

      spark must not beNull
    }

    "ingest chicago" >> {
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

    "ingest geonames" >> {
      SparkSQLTestUtils.ingestGeoNames(dsParams)

      gndf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "geonames")
        .load()
      gndf.printSchema()
      gndf.createOrReplaceTempView("geonames")

      gndf.collect().length mustEqual 2550
    }

    "ingest states" >> {
      import org.apache.spark.sql.functions.broadcast

      SparkSQLTestUtils.ingestStates(ds)

      sdf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "states")
        .load()
      sdf.printSchema()
      sdf.createOrReplaceTempView("states")

      broadcast(sdf).createOrReplaceTempView("broadcastStates")

      sdf.collect.length mustEqual 56


    }

    "basic sql 1" >> {
      val r = sc.sql("select * from chicago where case_number = 1")
      r.show()
      val d = r.collect()

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
    }


    "join st_contains" >> {
      val r = sc.sql(
        """
          |select broadcastStates.STUSPS, count(*), sum(population)
          |from geonames, broadcastStates
          |where st_contains(broadcastStates.the_geom, geonames.geom)
          |  and featurecode = "PPL"
          |group by broadcastStates.STUSPS
          |order by broadcastStates.STUSPS
        """.stripMargin
      )
      val d = r.collect()
      val d1 = d.head
      d.length mustEqual 48
      d1.getAs[String](0) mustEqual "AL"
      d1.getAs[Long](1) mustEqual 6
      d1.getAs[Long](2) mustEqual 2558
    }

    "join distance to centroid" >> {
      val r = sc.sql(
        """
          |select geonames.name,
          |       stateCentroids.abbrev,
          |       st_distanceSpheroid(stateCentroids.geom, geonames.geom) as dist
          |from geonames,
          |     (select broadcastStates.STUSPS as abbrev,
          |      st_centroid(broadcastStates.the_geom) as geom
          |      from broadcastStates) as stateCentroids
          |where geonames.admin1code = stateCentroids.abbrev
          |order by dist
        """.stripMargin)
      r.show(100)

      true mustEqual true
    }

    "st_translate" >> {
      val r = sc.sql(
        """
          |select ST_Translate(st_geomFromWKT('POINT(0 0)'), 5, 12)
        """.stripMargin)

      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(5 12)")
    }

    "st_geomFromWKT" >> {

      "point" >> {
        val r = sc.sql(
          """
            |select st_geomFromWKT('POINT(0 0)')
          """.stripMargin
        )

        r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
      }

      "lineString" >> {
        val lineString = sc.sql(
          """
            |select st_geomFromWKT('LINESTRING(0 0, 1 1, 2 2)')
          """.stripMargin
        )

        lineString.collect().head.getAs[LineString](0) mustEqual
          WKTUtils.read("LINESTRING(0 0, 1 1, 2 2)").asInstanceOf[LineString]
      }

      "polygon" >> {
        val r = sc.sql(
          """
            |select st_geomFromWKT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')
          """.stripMargin
        )

        r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
          "2.0 2.0, 0.0 2.0, 0.0 0.0))")
      }

      /*"multiPolygon" >> {
        val r = sc.sql(
          """
            |select st_geomFromWKT('MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 )),((-4 4, 4 4, 4 -4, -4 -4, -4 4),
            |                                    (2 2, -2 2, -2 -2, 2 -2, 2 2)))')
          """.stripMargin
        )

        r.collect().head.getAs[MultiPolygon](0) mustEqual
          WKTUtils.read("MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 ))," +
            "((-4 4, 4 4, 4 -4, -4 -4, -4 4),(2 2, -2 2, -2 -2, 2 -2, 2 2)))")
      }*/
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

    "st_pointFromText" >> {
      val r = sc.sql(
        """
          |select st_pointFromText('Point(0 0)')
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
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
  }
    //  import spark.sqlContext.{sql => $}
//
//  //$("select * from chicago where (dtg >= cast('2016-01-01' as timestamp) and dtg <= cast('2016-02-01' as timestamp))").show()
//  //$("select * from chicago where arrest = 'true' and (dtg >= cast('2016-01-01' as timestamp) and dtg <= cast('2016-02-01' as timestamp)) and st_contains(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))").show()
//  //$("select st_castToPoint(st_geomFromWKT('POINT(-77 38)')) as p").show()
//  //$("select st_contains(st_castToPoint(st_geomFromWKT('POINT(-77 38)')),st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))").show()
//
//  //$("select st_centroid(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))')),arrest from chicago limit 10").show()
//
//  //  $("select arrest,case_number,geom from chicago limit 5").show()
//
//  //select  arrest, geom, st_centroid(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//
//  import org.apache.spark.sql.functions.broadcast
//

//
//  broadcast(sdf).createOrReplaceTempView("broadcastStates")
//




//  println(s"time: ${new DateTime}")
//
//  $(
//    """
//      |  select geonames.name, states.STUSPS
//      |  from geonames, states
//      |  where st_contains(states.the_geom, geonames.geom)
//    """.stripMargin).show(100, false)
//
//  println(s"time: ${new DateTime}")
//
//  $(
//    """
//      |  explain select geonames.name, states.STUSPS
//      |  from geonames, states
//      |  where st_contains(states.the_geom, geonames.geom)
//    """.stripMargin).show(100, false)
//
//  println(s"*** Length of States DF:  ${sdf.collect().length}")
////
////  System.exit(0)
////
//  val qdf = $(
//    """
//      | select STUSPS, NAME
//      | from states
//      | order By(name)
//    """.stripMargin) //.show(100)
//
//  $(
//    """
//      | select *
//      | from chicago
//      | where
//      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  $(
//    """
//      | select arrest, geom
//      | from chicago
//      | where
//      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  $(
//    """
//      | select st_convexhull(geom)
//      | from chicago
//      | where
//      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  println("Testing predicates")
//
//  $("""
//      |select  arrest, geom
//      |from    chicago
//      |where
//      |  st_contains(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'), geom)
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  $("""
//      |select  arrest, geom
//      |from    chicago
//      |where
//      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  $("""
//      |select  arrest, geom
//      |from    chicago
//      |where
//      |  st_intersects(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  $("""
//      |select  arrest, geom
//      |from    chicago
//      |where
//      |  st_within(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  println("Done testing predicates")
//
//  println("Compute distances")
//  $(
//    """
//      |select st_distanceSpheroid(geom, st_geomFromWKT('POINT(-78 37)')) as dist
//      |from chicago
//    """.stripMargin).show()
//
//  $("""
//      |select  arrest, geom, st_contains(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'), geom) as contains,
//      |                      st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))')) as crosses,
//      |                      st_intersects(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))')) as intersects
//      |
//      |from    chicago
//      |where
//      |  st_within(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
//      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
//    """.stripMargin).show()
//
//  val res: DataFrame = $(
//    """
//      |select __fid__ as id,arrest,geom from chicago
//    """.stripMargin)
//
//  val results = res.collect()
//  results.length
//  res.show(false)
//
//  res.write
//    .format("geomesa")
//    .options(dsParams)
//    //    .option(GM.instanceIdParam.getName, instanceName)
//    //    .option(GM.userParam.getName, "root")
//    //    .option(GM.passwordParam.getName, "password")
//    //    .option(GM.tableNameParam.getName, "sparksql")
//    //    .option(GM.zookeepersParam.getName, mac.getZooKeepers)
//    .option("geomesa.feature", "chicago2")
//    .save()
//
//
//  println(s"After the save: ${ds.getTypeNames.mkString(", ")}")
//
//  /*
//    res.show()
//  */
//
//  /*
//    res
//      .where("st_contains(geom, st_geomFromWKT('POLYGON((-78 38.1,-76 38.1,-76 39,-78 39,-78 38.1))'))")
//      .select("id").show()
//  */
//
//  val dataset = $(
//    """
//      |select st_makeBox2D(ll,ur) as bounds from (select p[0] as ll,p[1] as ur from (select collect_list(geom) as p from chicago group by arrest))
//    """.stripMargin)
//
//
//  dataset.show(false)
//  val bounds = dataset.collect.map {
//    case Row(bounds: Geometry) => println("Got geometry")
//      bounds
//  }.apply(0)
//  println(s"Bounds = $bounds")
}
