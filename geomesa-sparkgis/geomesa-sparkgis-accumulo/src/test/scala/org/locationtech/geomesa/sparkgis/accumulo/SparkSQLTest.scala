package org.locationtech.geomesa.sparkgis.accumulo

import java.nio.file.Files

import com.vividsolutions.jts.geom.{Coordinate, Geometry}
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloConfig}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, DataUtilities, Query}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams => GM}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.tools.ingest.ConverterIngest
import org.locationtech.geomesa.utils.geotools.{GeneralShapefileIngest, SimpleFeatureTypes}

import scala.collection.JavaConversions._

object SparkSQLTest extends App {

  System.setProperty(QueryProperties.SCAN_RANGES_TARGET.property, "1")
  System.setProperty(AccumuloQueryProperties.SCAN_BATCH_RANGES.property, s"${Int.MaxValue}")

  val randomDir = Files.createTempDirectory("mac").toFile
  val config = new MiniAccumuloConfig(randomDir, "password").setJDWPEnabled(true)
  val mac = new MiniAccumuloCluster(config)
  mac.start()
  val instanceName = mac.getInstanceName
  val connector = mac.getConnector("root", "password")

  val dsParams: Map[String, String] = Map(
    //    "connector" -> connector,
    GM.zookeepersParam.getName -> mac.getZooKeepers,
    GM.instanceIdParam.getName -> instanceName,
    GM.userParam.getName -> "root",
    GM.passwordParam.getName -> "password",
    "caching"   -> "false",
    // note the table needs to be different to prevent testing errors
    GM.tableNameParam.getName -> "sparksql")

  val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

  // GeoNames ingest
  val ingest = new ConverterIngest(GeoNames.sft, dsParams, GeoNames.conf, Seq("/home/mzimmerman/sparksql/sample2.txt"), "", Iterator.empty, 16)
  ingest.run


  // States shapefile ingest
  GeneralShapefileIngest.shpToDataStore("/home/mzimmerman/sparksql/states.shp", ds, "states")

  val sft = SimpleFeatureTypes.createType("chicago", "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")
  ds.createSchema(sft)

  val fs = ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore]

  val parseDate = ISODateTimeFormat.basicDateTime().parseDateTime _
  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  val features = DataUtilities.collection(List(
    new ScalaSimpleFeature("1", sft, initialValues = Array("true","1",parseDate("20160101T000000.000Z").toDate, createPoint(new Coordinate(-76.5, 38.5)))),
    new ScalaSimpleFeature("2", sft, initialValues = Array("true","2",parseDate("20160102T000000.000Z").toDate, createPoint(new Coordinate(-77.0, 38.0)))),
    new ScalaSimpleFeature("3", sft, initialValues = Array("true","3",parseDate("20160103T000000.000Z").toDate, createPoint(new Coordinate(-78.0, 39.0))))
  ))

  fs.addFeatures(features)

  System.setProperty("sun.net.spi.nameservice.nameservers", "192.168.2.77")
  System.setProperty("sun.net.spi.nameservice.provider.1", "dns,sun")

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  println(s"DS typenames: ${ds.getTypeNames.mkString(", ")}.")
  val fs2 = ds.getFeatureSource("geonames")
  println(s" GeoNames count: ${fs2.getCount(Query.ALL)}")

  val df: DataFrame = spark.read
    .format("geomesa")
    .options(dsParams)
    .option("geomesa.feature", "chicago")
    .load()

  df.printSchema()

  df.createOrReplaceTempView("chicago")

    val gndf: DataFrame = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "geonames")
      .load()

    gndf.printSchema()

    gndf.createOrReplaceTempView("geonames")
  println(s"*** Length of GeoNames DF:  ${gndf.collect().length}")

    val sdf: DataFrame = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "states")
      .load()

    sdf.printSchema()
    sdf.createOrReplaceTempView("states")
    println(s"*** Length of States DF:  ${sdf.collect().length}")

  import spark.sqlContext.{sql => $}

  //$("select * from chicago where (dtg >= cast('2016-01-01' as timestamp) and dtg <= cast('2016-02-01' as timestamp))").show()
  //$("select * from chicago where arrest = 'true' and (dtg >= cast('2016-01-01' as timestamp) and dtg <= cast('2016-02-01' as timestamp)) and st_contains(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))").show()
  //$("select st_castToPoint(st_geomFromWKT('POINT(-77 38)')) as p").show()
  //$("select st_contains(st_castToPoint(st_geomFromWKT('POINT(-77 38)')),st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))").show()

  //$("select st_centroid(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))')),arrest from chicago limit 10").show()

  //  $("select arrest,case_number,geom from chicago limit 5").show()

  //select  arrest, geom, st_centroid(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))

  import org.apache.spark.sql.functions.broadcast

  spark.sqlContext.setConf("spark.sql.crossJoin.enabled", "true")

  broadcast(sdf).createOrReplaceTempView("broadcastStates")

  $(
    """
      |  explain select geonames.name, broadcastStates.STUSPS
      |  from geonames, broadcastStates
      |  where st_contains(broadcastStates.the_geom, geonames.geom)
    """.stripMargin).show(100, false)

  println(s"time: ${new DateTime}")
  $(
    """
      |  select geonames.name, broadcastStates.STUSPS
      |  from geonames, broadcastStates
      |  where st_contains(broadcastStates.the_geom, geonames.geom)
    """.stripMargin).show(100, false)

  def executeSQL(query: String, rows: Integer, explain: Boolean) = {
    $(query).show(rows, false)
    if (explain) {
      $("explain "+query).show(1, false)
    }
  }

  val sqlGroupBy = """
    |  select broadcastStates.STUSPS, count(*), sum(population)
    |  from geonames, broadcastStates
    |  where st_contains(broadcastStates.the_geom, geonames.geom)
    |        and featurecode = "PPL"
    |  group by broadcastStates.STUSPS
    |  order by broadcastStates.STUSPS
  """.stripMargin

  executeSQL(sqlGroupBy, 100, true)

  println(s"time: ${new DateTime}")

  $(
    """
      |  select geonames.name, states.STUSPS
      |  from geonames, states
      |  where st_contains(states.the_geom, geonames.geom)
    """.stripMargin).show(100, false)

  println(s"time: ${new DateTime}")

  $(
    """
      |  explain select geonames.name, states.STUSPS
      |  from geonames, states
      |  where st_contains(states.the_geom, geonames.geom)
    """.stripMargin).show(100, false)

  println(s"*** Length of States DF:  ${sdf.collect().length}")
//
//  System.exit(0)
//
  val qdf = $(
    """
      | select STUSPS, NAME
      | from states
      | order By(name)
    """.stripMargin) //.show(100)

  $(
    """
      | select *
      | from chicago
      | where
      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  $(
    """
      | select arrest, geom
      | from chicago
      | where
      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  $(
    """
      | select st_convexhull(geom)
      | from chicago
      | where
      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  println("Testing predicates")

  $("""
      |select  arrest, geom
      |from    chicago
      |where
      |  st_contains(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'), geom)
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  $("""
      |select  arrest, geom
      |from    chicago
      |where
      |  st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  $("""
      |select  arrest, geom
      |from    chicago
      |where
      |  st_intersects(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  $("""
      |select  arrest, geom
      |from    chicago
      |where
      |  st_within(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  println("Done testing predicates")

  println("Compute distances")
  $(
    """
      |select st_distanceSpheroid(geom, st_geomFromWKT('POINT(-78 37)')) as dist
      |from chicago
    """.stripMargin).show()

  $("""
      |select  arrest, geom, st_contains(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'), geom) as contains,
      |                      st_crosses(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))')) as crosses,
      |                      st_intersects(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))')) as intersects
      |
      |from    chicago
      |where
      |  st_within(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()

  val res: DataFrame = $(
    """
      |select __fid__ as id,arrest,geom from chicago
    """.stripMargin)

  val results = res.collect()
  results.length
  res.show(false)

  res.write
    .format("geomesa")
    .options(dsParams)
    //    .option(GM.instanceIdParam.getName, instanceName)
    //    .option(GM.userParam.getName, "root")
    //    .option(GM.passwordParam.getName, "password")
    //    .option(GM.tableNameParam.getName, "sparksql")
    //    .option(GM.zookeepersParam.getName, mac.getZooKeepers)
    .option("geomesa.feature", "chicago2")
    .save()


  println(s"After the save: ${ds.getTypeNames.mkString(", ")}")

  /*
    res.show()
  */

  /*
    res
      .where("st_contains(geom, st_geomFromWKT('POLYGON((-78 38.1,-76 38.1,-76 39,-78 39,-78 38.1))'))")
      .select("id").show()
  */

  val dataset = $(
    """
      |select st_makeBox2D(ll,ur) as bounds from (select p[0] as ll,p[1] as ur from (select collect_list(geom) as p from chicago group by arrest))
    """.stripMargin)


  dataset.show(false)
  val bounds = dataset.collect.map {
    case Row(bounds: Geometry) => println("Got geometry")
      bounds
  }.apply(0)
  println(s"Bounds = $bounds")
}
