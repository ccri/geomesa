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

  sequential

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
      "nested GroupBy Count() and" >> {
        val groupByCountMatcher = """^\[(\{ "\d" : \[(\{ "." : \{ "count": \d \}\},?)+\]\},?)*\]$"""

        "be empty initially" >> {
          val groupBy = newStat[Int]("cat1","GroupBy(cat2,Count())", false)
          groupBy.toJson mustEqual "[]"
          groupBy.isEmpty must beTrue
        }

        "observe correct values" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          groupBy.groupedStats.size mustEqual 10
          val nestedGroupBy = groupBy.groupedStats.get(0).get.asInstanceOf[GroupBy[String]]
          nestedGroupBy.groupedStats.get("S").get.asInstanceOf[CountStat].counter mustEqual 1L
        }

        "unobserve correct values" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          groupBy.groupedStats.size mustEqual 10
          features.take(10).foreach(groupBy.unobserve)
          val nestedGroupBy = groupBy.groupedStats.get(0).get.asInstanceOf[GroupBy[String]]
          nestedGroupBy.groupedStats.get("S").get.asInstanceOf[CountStat].counter mustEqual 1L
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          groupBy.toJson must beMatching (groupByCountMatcher)
        }

        "serialize and deserialize" >> {
          "observed" >> {
            val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            // Sometimes Json is deserialized in a different order making direct comparison not possible
            groupBy.toJson must beMatching (groupByCountMatcher)
            unpacked.toJson must beMatching (groupByCountMatcher)
          }
          "unobserved" >> {
            val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())", false)
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            // Sometimes Json is deserialized in a different order making direct comparison not possible
            groupBy.toJson must beMatching (groupByCountMatcher)
            unpacked.toJson must beMatching (groupByCountMatcher)
          }
        }

        "deserialize as immutable value" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          val packed = StatSerializer(sft).serialize(groupBy)
          val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
          // Sometimes Json is deserialized in a different order making direct comparison not possible
          groupBy.toJson must beMatching (groupByCountMatcher)
          unpacked.toJson must beMatching (groupByCountMatcher)

          unpacked.clear must throwAn[Exception]
          unpacked.+=(groupBy) must throwAn[Exception]
          unpacked.observe(features.head) must throwAn[Exception]
          unpacked.unobserve(features.head) must throwAn[Exception]
        }

        "combine two stats" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          val groupBy2 = newStat[Int]("cat1", "GroupBy(cat2,Count())", false)

          features2.foreach { groupBy2.observe }

          groupBy2.groupedStats.size mustEqual 10

          groupBy += groupBy2

          groupBy.groupedStats.size mustEqual 10
          groupBy2.groupedStats.size mustEqual 10
        }

        "clear" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          groupBy.isEmpty must beFalse

          groupBy.clear()

          groupBy.groupedStats.size mustEqual 0L
          groupBy.isEmpty must beTrue
        }
      }

      "Count Stat and" >> {
        "be empty initially" >> {
          val groupBy = newStat[Int]("cat1","Count()", false)
          groupBy.toJson mustEqual "[]"
          groupBy.isEmpty must beTrue
        }

        "observe correct values" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          groupBy.groupedStats.size mustEqual 10
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 10L
        }

        "unobserve correct values" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          groupBy.groupedStats.size mustEqual 10
          features.take(10).foreach(groupBy.unobserve)
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 9L
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          groupBy.toJson must beMatching ("""^\[(\{ "\d" : \{ "count": 10 \}\},?){10}\]$""")
        }

        "serialize and deserialize" >> {
          "observed" >> {
            val groupBy = newStat[Int]("cat1", "Count()")
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            groupBy.toJson mustEqual unpacked.toJson
          }
          "unobserved" >> {
            val groupBy = newStat[Int]("cat1", "Count()", false)
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            groupBy.toJson mustEqual unpacked.toJson
          }
        }

        "deserialize as immutable value" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          val packed = StatSerializer(sft).serialize(groupBy)
          val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
          unpacked.toJson mustEqual groupBy.toJson

          unpacked.clear must throwAn[Exception]
          unpacked.+=(groupBy) must throwAn[Exception]
          unpacked.observe(features.head) must throwAn[Exception]
          unpacked.unobserve(features.head) must throwAn[Exception]
        }

        "combine two stats" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          val groupBy2 = newStat[Int]("cat1", "Count()", false)

          features2.foreach { groupBy2.observe }

          groupBy2.groupedStats.size mustEqual 10

          groupBy += groupBy2

          groupBy.groupedStats.size mustEqual 10
          groupBy2.groupedStats.size mustEqual 10
        }

        "clear" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          groupBy.isEmpty must beFalse

          groupBy.clear()

          groupBy.groupedStats.size mustEqual 0L
          groupBy.isEmpty must beTrue
        }
      }

      "MinMax Stat and" >> {
        "be empty initially" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)", false)
          groupBy.toJson mustEqual "[]"
          groupBy.isEmpty must beTrue
        }

        "observe correct values" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)")
          val groupedStats0 = groupBy.groupedStats.getOrElse(0, null).asInstanceOf[MinMax[String]]
          groupedStats0.bounds mustEqual ("abc000", "abc090")
          groupedStats0.cardinality must beCloseTo(100L, 5)
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)")
          groupBy.toJson must beMatching ("""^\[(\{ "\d" : \{ "min": "abc[0-9]{3}", "max": "abc[0-9]{3}", "cardinality": \d+ \}\},?){10}\]$""")
        }

        "serialize empty to json" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)", false)
          groupBy.toJson mustEqual "[]"
        }

        "serialize and deserialize" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)")
          val packed = StatSerializer(sft).serialize(groupBy)
          val unpacked = StatSerializer(sft).deserialize(packed)
          unpacked.toJson mustEqual groupBy.toJson
        }

        "serialize and deserialize empty MinMax" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)", false)
          val packed = StatSerializer(sft).serialize(groupBy)
          val unpacked = StatSerializer(sft).deserialize(packed)
          unpacked.toJson mustEqual groupBy.toJson
        }

        "deserialize as immutable value" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)")
          val packed = StatSerializer(sft).serialize(groupBy)
          val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
          unpacked.toJson mustEqual groupBy.toJson

          unpacked.clear must throwAn[Exception]
          unpacked.+=(groupBy) must throwAn[Exception]
          unpacked.observe(features.head) must throwAn[Exception]
          unpacked.unobserve(features.head) must throwAn[Exception]
        }

        "combine two MinMaxes" >> {
          val groupBy1 = newStat[Int]("cat1","MinMax(strAttr)")
          val groupBy2 = newStat[Int]("cat1","MinMax(strAttr)", false)

          features2.foreach { groupBy2.observe }
          val gS20 = groupBy2.groupedStats.get(0).getOrElse(null).asInstanceOf[MinMax[String]]
          gS20.bounds mustEqual ("abc100", "abc190")
          gS20.cardinality must beCloseTo(100L, 5)

          groupBy1 += groupBy2
          val gS10 = groupBy1.groupedStats.get(0).getOrElse(null).asInstanceOf[MinMax[String]]
          gS10.bounds mustEqual ("abc000", "abc190")
          gS10.cardinality must beCloseTo(200L, 5)
          gS20.bounds mustEqual ("abc100", "abc190")
        }

        "clear" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)")
          groupBy.isEmpty must beFalse

          groupBy.clear()

          groupBy.isEmpty must beTrue
          groupBy.groupedStats.size mustEqual 0
        }
      }

      "Enumeration Stat and" >> {

      }

      "TopK Stat and" >> {

      }

      "Frequency Stat and" >> {

      }

      "Z3Frequency Stat and" >> {

      }

      "Histogram Stat and" >> {

      }

      "Z3Histogram Stat and" >> {

      }

      "SeqStat Stat and" >> {

      }
    }
  }
}
