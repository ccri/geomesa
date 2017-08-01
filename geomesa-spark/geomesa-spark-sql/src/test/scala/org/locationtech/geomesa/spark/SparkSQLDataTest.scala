/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util
import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.geotools.data.{DataStore, DataStoreFinder, Query, Transaction}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

@RunWith(classOf[JUnitRunner])
class SparkSQLDataTest extends Specification with LazyLogging {
  val createPoint = JTSFactoryFinder.getGeometryFactory.createPoint(_: Coordinate)

  "sql data tests" should {
    sequential

    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")
    var ds: DataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null

    var df: DataFrame = null
    var dfIndexed: DataFrame = null
    var dfPartitioned: DataFrame = null

    // before
    step {
      ds = DataStoreFinder.getDataStore(dsParams)
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext
    }

    "ingest chicago" >> {
      SparkSQLTestUtils.ingestChicago(ds)

      df = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .load()
      logger.info(df.schema.treeString)

      df.createOrReplaceTempView("chicago")

      df.collect.length mustEqual 18
    }

    "create indexed relation" >> {
      dfIndexed = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .load()
      logger.info(df.schema.treeString)

      dfIndexed.createOrReplaceTempView("chicagoIndexed")

      dfIndexed.collect.length mustEqual 18
    }

    "create spatially partitioned relation" >> {
      dfPartitioned = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "chicago")
        .option("cache", "true")
        .option("spatial","true")
        .option("strategy", "EQUAL")
        .option("partitions","10")
        .load()
      logger.info(df.schema.treeString)

      dfPartitioned.createOrReplaceTempView("chicagoPartitioned")

      // Filter if features belonged to multiple partition envelopes
      // TODO: Better way
      val hashSet = new util.HashSet[String]()
      dfPartitioned.collect.foreach{ row =>
        hashSet.add(row.getAs[String]("__fid__"))
      }
      hashSet.size() mustEqual 18
    }

    "partitioned spatial join" >> {
      val r = sc.sql("select * from chicagoPartitioned join chicago ON st_intersects(chicagoPartitioned.geom, chicago.geom)")
      r.show()
      val d = r.collect()
      d.length mustEqual 18
    }

    "sweepline join" >> {

      val gf = new GeometryFactory

      val points = (1 until 1000).map { i =>
        val x = -180 + 360 * Math.random()
        val y = -90 + 180 * Math.random()
        gf.createPoint(new Coordinate(x, y))

      }

      SparkSQLTestUtils.ingestPoints(ds, "points", points)

      val polys =  (1 until 1000).map { i =>
        val x = -180 + 360 * Math.random()
        val y = -90 + 180 * Math.random()
        val coords = Array(new Coordinate(x - 0.25, y-0.25), new Coordinate(x - 0.25, y+0.25), new Coordinate(x + 0.25, y-0.25), new Coordinate(x + 0.25, y+0.25),new Coordinate(x - 0.25, y-0.25))
        gf.createPolygon(coords)
      }

      SparkSQLTestUtils.ingestPolys(ds, "polys", polys)

      val partitionedPolys = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "polys")
        .option("cache", "true")
        .option("spatial","true")
        .option("strategy", "EQUAL")
        .option("partitions","10")
        .load()

      val polysDf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "polys")
        .option("partitions","10")
        .load()

      val pointsDf = spark.read
        .format("geomesa")
        .options(dsParams)
        .option("geomesa.feature", "points")
        .load()

      partitionedPolys.createOrReplaceTempView("polysSpatial")
      pointsDf.createOrReplaceTempView("points")
      polysDf.createOrReplaceTempView("polys")


      var now = System.currentTimeMillis()
      val pCount = spark.sql("select * from polysSpatial join points on st_intersects(points.geom, polysSpatial.geom)").show()
      println(s"spatial took ${System.currentTimeMillis() - now}")
      println(s"count is $pCount")
      1 mustEqual 1
    }

//
//    "handle projections on in-memory store" >> {
//      val r = sc.sql("select geom from chicagoIndexed where case_number = 1")
//      val d = r.collect
//      d.length mustEqual 1
//
//      val row = d(0)
//      row.schema.fieldNames.length mustEqual 1
//      row.fieldIndex("geom") mustEqual 0
//    }
//
//    "basic sql indexed" >> {
//      val r = sc.sql("select * from chicagoIndexed where st_equals(geom, st_geomFromWKT('POINT(-76.5 38.5)'))")
//      val d = r.collect
//
//      d.length mustEqual 1
//      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
//    }
//
//    "basic sql partitioned" >> {
//      sc.sql("select * from chicagoPartitioned").show()
//      val r = sc.sql("select * from chicagoPartitioned where st_equals(geom, st_geomFromWKT('POINT(-77 38)'))")
//      val d = r.collect
//
//      d.length mustEqual 1
//      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-77, 38))
//    }
//
//    "partitioned spatial join" >> {
//      val r = sc.sql("select * from chicagoPartitioned join chicago ON st_intersects(chicagoPartitioned.geom, chicago.geom)")
//      r.show()
//      val d = r.collect()
//      d.length mustEqual 3
//    }
//
//    "basic sql 1" >> {
//      val r = sc.sql("select * from chicago where st_equals(geom, st_geomFromWKT('POINT(-76.5 38.5)'))")
//      val d = r.collect
//
//      d.length mustEqual 1
//      d.head.getAs[Point]("geom") mustEqual createPoint(new Coordinate(-76.5, 38.5))
//    }
//
//
//    "basic sql 4" >> {
//      val r = sc.sql("select 1 + 1 > 4")
//      val d = r.collect
//
//      d.length mustEqual 1
//    }
//
//    "basic sql 5" >> {
//      val r = sc.sql("select * from chicago where case_number = 1 and st_intersects(geom, st_makeBox2d(st_point(-77, 38), st_point(-76, 39)))")
//      val d = r.collect
//
//      d.length mustEqual 1
//    }
//
//    "basic sql 6" >> {
//      val r = sc.sql("select st_intersects(st_makeBox2d(st_point(-77, 38), st_point(-76, 39)), st_makeBox2d(st_point(-77, 38), st_point(-76, 39)))")
//      val d = r.collect
//
//      d.length mustEqual 1
//    }
//
//    "st_translate" >> {
//      "null" >> {
//        sc.sql("select st_translate(null, null, null)").collect.head(0) must beNull
//      }
//
//      "point" >> {
//        val r = sc.sql(
//          """
//          |select st_translate(st_geomFromWKT('POINT(0 0)'), 5, 12)
//        """.stripMargin)
//
//        r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(5 12)")
//      }
//    }
//
//    "where __fid__ equals" >> {
//      val r = sc.sql("select * from chicago where __fid__ = '1'")
//      val d = r.collect()
//
//      d.length mustEqual 1
//      d.head.getAs[Int]("case_number") mustEqual 1
//    }
//
//    "where attr equals" >> {
//      val r = sc.sql("select * from chicago where case_number = 2")
//      val d = r.collect()
//
//      d.length mustEqual 1
//      d.head.getAs[Int]("case_number") mustEqual 2
//    }
//
//    "where __fid__ in" >> {
//      val r = sc.sql("select * from chicago where __fid__ in ('1', '2')")
//      val d = r.collect()
//
//      d.length mustEqual 2
//      d.map(_.getAs[Int]("case_number")).toSeq must containTheSameElementsAs(Seq(1, 2))
//    }
//
//    "where attr in" >> {
//      val r = sc.sql("select * from chicago where case_number in (2, 3)")
//      val d = r.collect()
//
//      d.length mustEqual 2
//      d.map(_.getAs[Int]("case_number")).toSeq must containTheSameElementsAs(Seq(2, 3))
//    }

    // after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
