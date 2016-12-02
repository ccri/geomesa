package org.locationtech.geomesa.sparkgis.accumulo

import java.util.{Map => JMap}

import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.apache.spark.sql.{SQLContext, DataFrame, SparkSession}
import org.geotools.data.DataStoreFinder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.index.conf.QueryProperties
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
      SparkSQLTestUtils.ingestStates(ds)

      sdf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "states")
        .load()
      sdf.printSchema()
      sdf.createOrReplaceTempView("states")

      sdf.collect.length mustEqual 56
    }

    "basic sql 1" >> {
      val r = sc.sql("select * from chicago where case_number = 1")
      r.show()
      val d = r.collect()

      d.length mustEqual 1
      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
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
//  spark.sqlContext.setConf("spark.sql.crossJoin.enabled", "true")
//
//  broadcast(sdf).createOrReplaceTempView("broadcastStates")

//  $(
//    """
//      |  explain select geonames.name, broadcastStates.STUSPS
//      |  from geonames, broadcastStates
//      |  where st_contains(broadcastStates.the_geom, geonames.geom)
//    """.stripMargin).show(100, false)
//
//  println(s"time: ${new DateTime}")
//  $(
//    """
//      |  select geonames.name, broadcastStates.STUSPS
//      |  from geonames, broadcastStates
//      |  where st_contains(broadcastStates.the_geom, geonames.geom)
//    """.stripMargin).show(100, false)

//  def executeSQL(query: String, rows: Integer, explain: Boolean) = {
//    $(query).show(rows, false)
//    if (explain) {
//      $("explain "+query).show(1, false)
//    }
//  }
//
//  // find number of populated places (PPL) in the geonames set
//  // in each state, and sum their population
//  val sqlGroupBy = """
//    |  select broadcastStates.STUSPS, count(*), sum(population)
//    |  from geonames, broadcastStates
//    |  where st_contains(broadcastStates.the_geom, geonames.geom)
//    |        and featurecode = "PPL"
//    |  group by broadcastStates.STUSPS
//    |  order by broadcastStates.STUSPS
//  """.stripMargin
//
//  executeSQL(sqlGroupBy, 100, true)
//
//  // find centroid of each state
//  val sqlStateCentroid =
//    """
//      |select broadcastStates.STUSPS, st_centroid(broadcastStates.the_geom)
//      |from broadcastStates
//    """.stripMargin
//
//  executeSQL(sqlStateCentroid, 100, true)
//
//  // find points in geonames closest to the centroid of their state
//  val sqlDistanceToCentroid =
//    """
//      |select geonames.name,
//      |       stateCentroids.abbrev,
//      |       st_distanceSpheroid(stateCentroids.geom, geonames.geom) as dist
//      |from geonames,
//      |     (select broadcastStates.STUSPS as abbrev,
//      |      st_centroid(broadcastStates.the_geom) as geom
//      |      from broadcastStates) as stateCentroids
//      |where geonames.admin1code = stateCentroids.abbrev
//      |order by dist
//    """.stripMargin
//
//  executeSQL(sqlDistanceToCentroid, 100, true)
//
//  val sqlConstant = "select pi()"
//  executeSQL(sqlConstant, 100, true)
//
//  val sql_st_translate =
//    """
//      |select ST_Translate(st_geomFromWKT('POINT(0 0)'), 5, 12)
//    """.stripMargin
//  executeSQL(sql_st_translate, 100, true)



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
