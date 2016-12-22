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
import org.apache.spark.sql._
import org.geotools.data.{DataStore, DataStoreFinder}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricOutputsTest extends Specification with LazyLogging {

  "sql geometry constructors" should {
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

    "st_asBinary" >> {
      val r = sc.sql(
        """
          |select st_asBinary(geom) from chicago
        """.stripMargin
      )
      println(r.collect().head.getAs[Array[Byte]](0))
      success
    }

    /*"st_asGeoJSON" >> {
      val r = sc.sql(
        """
          |select st_asGeoJSON(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      println(r.collect().head.getAs[String](0))
      success
    }*/

    "st_asLatLonText" >> {
      val r = sc.sql(
        """
          |select st_asLatLonText(geom) from chicago
        """.stripMargin
      )
      r.collect().head.getAs[String](0) mustEqual "77°30\'0.000\"W 38°30\'0.000\"N"
    }

    "st_asText" >> {
      val r = sc.sql(
        """
          |select st_asText(geom) from chicago
        """.stripMargin
      )
      r.collect().head.getAs[String](0) mustEqual "POINT (-76.5 38.5)"
    }


    //after
    step {
      ds.dispose()
      spark.stop()
    }
  }
}
