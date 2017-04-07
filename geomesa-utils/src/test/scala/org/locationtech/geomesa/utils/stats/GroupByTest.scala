/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod, Z2SFC}
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.sfcurve.zorder.Z2
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GroupByTest extends Specification with StatTestHelper {

  def newStat[T](attribute: String, groupedStat: String, observe: Boolean = true): GroupBy[T] = {
    val stat = Stat(sft, s"GroupBy($attribute,$groupedStat)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[GroupBy[T]]
  }

  def groupByStat[T](attribute: T, groupedStat: Stat, observe: Boolean = true): GroupBy[T] = {
    val stat = Stat(sft, s"GroupBy($attribute,$groupedStat)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[GroupBy[T]]
  }

  "GroupBy Stat" should {
    "work with" >> {
      "Count Stat and" >> {
        "be empty initially" >> {
          val groupBy = newStat[Int]("cat","Count()", false)
          groupBy.toJson mustEqual "[]"
          groupBy.isEmpty must beTrue
        }

        "observe correct values" >> {
          val groupBy = newStat[Int]("cat", "Count()")
          groupBy.groupedStats.size mustEqual 10
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 100L
        }

        "unobserve correct values" >> {
          val groupBy = newStat[Int]("cat", "Count()")
          groupBy.groupedStats.size mustEqual 10
          features.take(10).foreach(groupBy.unobserve)
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 90L
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat", "Count()")
          groupBy.toJson mustEqual """[{ "8" : { "count": 100 }},{ "2" : { "count": 100 }},{ "5" : { "count": 100 }},{ "4" : { "count": 100 }},{ "7" : { "count": 100 }},{ "1" : { "count": 100 }},{ "9" : { "count": 100 }},{ "3" : { "count": 100 }},{ "6" : { "count": 100 }},{ "0" : { "count": 100 }}]"""
        }

        "serialize and deserialize" >> {
          "observed" >> {
            val groupBy = newStat[Int]("cat", "Count()")
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            groupBy.toJson mustEqual unpacked.toJson
          }
          "unobserved" >> {
            val groupBy = newStat[Int]("cat", "Count()", false)
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            groupBy.toJson mustEqual unpacked.toJson
          }
        }

        "deserialize as immutable value" >> {
          val groupBy = newStat[Int]("cat", "Count()")
          val packed = StatSerializer(sft).serialize(groupBy)
          val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
          unpacked.toJson mustEqual groupBy.toJson

          unpacked.clear must throwAn[Exception]
          unpacked.+=(groupBy) must throwAn[Exception]
          unpacked.observe(features.head) must throwAn[Exception]
          unpacked.unobserve(features.head) must throwAn[Exception]
        }

        "combine two stats" >> {
          val groupBy = newStat[Int]("cat", "Count()")
          val groupBy2 = newStat[Int]("cat", "Count()", false)

          features2.foreach { groupBy2.observe }

          groupBy2.groupedStats.size mustEqual 10

          groupBy += groupBy2

          groupBy.groupedStats.size mustEqual 10
          groupBy2.groupedStats.size mustEqual 10
        }
      }
    }
  }
}
