package org.locationtech.geomesa.sparkgis.accumulo

import java.util.{Map => JMap}

import com.vividsolutions.jts.geom.{Coordinate, Point, Polygon}
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.sql.{Row, SQLContext, DataFrame, SparkSession}
import org.geotools.data.DataStoreFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.conf.QueryProperties
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

    val box1 = "POLYGON((-76.9 38, -76.9 39, -76.1 39, -76.1 38, -76.9 38))"


    def $(sql: String, n: Int = 100): Array[Row] = {
      val r = sc.sql(sql)
      println(sql)
      r.show(n)
      r.collect()
    }

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

    /*
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
    */

    "basic sql 1" >> {
      val r = sc.sql("select * from chicago where case_number = 1")
      r.show()
      val d = r.collect()

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
    }

    /*
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
    */

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

    /*
    "st_contains" >> {
      val r = $(
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
    */

    //  $(
    //    """
    //      | select st_convexhull(geom)
    //      | from chicago
    //      | where
    //      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
    //      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    //    """.stripMargin).show()

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

    "st_translate" >> {
      val r = sc.sql(
        """
          |select ST_Translate(st_geomFromWKT('POINT(0 0)'), 5, 12)
        """.stripMargin)

      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(5 12)")
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

//  println("Testing predicates")
//

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
