package org.locationtech.geomesa.sparkgis

import java.nio.file.Files

import com.vividsolutions.jts.geom.Coordinate
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloConfig}
import org.apache.spark.sql.SparkSession
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreParams => GM}
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

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

  val dsParams = Map(
//    "connector" -> connector,
    GM.zookeepersParam.getName -> mac.getZooKeepers,
    GM.instanceIdParam.getName -> instanceName,
    GM.userParam.getName -> "root",
    GM.passwordParam.getName -> "password",
    "caching"   -> false,
    // note the table needs to be different to prevent testing errors
    "tableName" -> "sparksql")

  val ds = DataStoreFinder.getDataStore(dsParams).asInstanceOf[AccumuloDataStore]

  val sft = SimpleFeatureTypes.createType("chicago", "arrest:String,case_number:Int,dtg:Date,*geom:Point:srid=4326")
  ds.createSchema(sft)

  val fs = ds.getFeatureSource("chicago").asInstanceOf[SimpleFeatureStore]

  val parseDate = ISODateTimeFormat.basicDateTime().parseDateTime _
  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  val features = DataUtilities.collection(List(
    new ScalaSimpleFeature("1", sft, initialValues = Array("true","1",parseDate("20160101T000000.000Z").toDate, createPoint(new Coordinate(-76.5, 38.5)))),
    new ScalaSimpleFeature("2", sft, initialValues = Array("true","2",parseDate("20160102T000000.000Z").toDate, createPoint(new Coordinate(-77.0, 38.0))))
  ))

  fs.addFeatures(features)

  System.setProperty("sun.net.spi.nameservice.nameservers", "192.168.2.77")
  System.setProperty("sun.net.spi.nameservice.provider.1", "dns,sun")

  val spark = SparkSession.builder().master("local[*]").getOrCreate()

  val df = spark.read
    .format("geomesa")
    .option(GM.instanceIdParam.getName, instanceName)
    .option(GM.userParam.getName, "root")
    .option(GM.passwordParam.getName, "password")
    .option(GM.tableNameParam.getName, "sparksql")
    .option(GM.zookeepersParam.getName, mac.getZooKeepers)
//    .option(GM.mockParam.getName, "true")
    .option("geomesa.feature", "chicago")
    .load()

//  df.printSchema()

  df.createOrReplaceTempView("chicago")

  import spark.sqlContext.{sql => $}

  //$("select * from chicago where (dtg >= cast('2016-01-01' as timestamp) and dtg <= cast('2016-02-01' as timestamp))").show()
  //$("select * from chicago where arrest = 'true' and (dtg >= cast('2016-01-01' as timestamp) and dtg <= cast('2016-02-01' as timestamp)) and st_contains(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))").show()
  //$("select st_castToPoint(st_geomFromWKT('POINT(-77 38)')) as p").show()
    //$("select st_contains(st_castToPoint(st_geomFromWKT('POINT(-77 38)')),st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))").show()

  //$("select st_centroid(st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))')),arrest from chicago limit 10").show()

//  $("select arrest,case_number,geom from chicago limit 5").show()

  $("""
      |select  arrest,geom
      |from    chicago
      |where
      |  st_contains(geom, st_geomFromWKT('POLYGON((-78 37,-76 37,-76 39,-78 39,-78 37))'))
      |  and dtg >= cast('2015-12-31' as timestamp) and dtg <= cast('2016-01-07' as timestamp)
    """.stripMargin).show()


  val res = $(
    """
      |select __fid__ as id,arrest,geom from chicago
    """.stripMargin)

/*
  res.show()
*/

/*
  res
    .where("st_contains(geom, st_geomFromWKT('POLYGON((-78 38.1,-76 38.1,-76 39,-78 39,-78 38.1))'))")
    .select("id").show()
*/

/*
  $(
    """
      |select st_makeBox2D(ll,ur) as bounds from (select p[0] as ll,p[1] as ur from (select collect_list(geom) as p from chicago group by arrest))
    """.stripMargin).show()
*/
}
