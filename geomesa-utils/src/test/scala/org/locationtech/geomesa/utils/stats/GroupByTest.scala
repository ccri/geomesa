/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong, Integer => jInt}
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
        "work with ints" >> {
          "be empty initiallly" >> {
            val groupBy = newStat[Int]("cat1","Enumeration(intAttr)", false)
            groupBy.toJson mustEqual "[]"
            groupBy.isEmpty must beTrue
          }

          "observe correct values" >> {
            val groupBy = newStat[Int]("cat1","Enumeration(intAttr)")
            forall(0 until 10){i =>
              val enumStat = groupBy.groupedStats.getOrElse(i, null).asInstanceOf[EnumerationStat[jInt]]
              enumStat.enumeration.forall(enum => enum._2 mustEqual 1)
            }
          }

          "serialize to json" >> {
            val groupBy = newStat[Int]("cat1","Enumeration(intAttr)")
            val packed   = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            val enums1 = groupBy.groupedStats.getOrElse(0, null).asInstanceOf[EnumerationStat[jInt]]
            val enums2 = unpacked.asInstanceOf[GroupBy[Int]].groupedStats.getOrElse(0, null).asInstanceOf[EnumerationStat[jInt]]

            enums2.attribute mustEqual enums1.attribute
            enums2.enumeration mustEqual enums1.enumeration
            enums2.size mustEqual enums1.size
            enums2.toJson mustEqual enums1.toJson
            groupBy.toJson mustEqual unpacked.toJson
          }

          "serialize empty to json" >> {
            val groupBy = newStat[Int]("cat1","Enumeration(intAttr)", false)
            groupBy.toJson mustEqual "[]"
          }

          "serialize and deserialize" >> {
            "observed" >> {
              val groupBy = newStat[Int]("cat1","Enumeration(intAttr)")
              val packed = StatSerializer(sft).serialize(groupBy)
              val unpacked = StatSerializer(sft).deserialize(packed)
              unpacked.toJson mustEqual groupBy.toJson
            }
            "unobserved" >> {
              val groupBy = newStat[Int]("cat1","Enumeration(intAttr)", false)
              val packed = StatSerializer(sft).serialize(groupBy)
              val unpacked = StatSerializer(sft).deserialize(packed)
              unpacked.toJson mustEqual groupBy.toJson
            }
          }

          "combine two stats" >> {
            val groupBy = newStat[Int]("cat1","Enumeration(intAttr)")
            val groupBy2 = newStat[Int]("cat1","Enumeration(intAttr)", false)

            features2.foreach { groupBy2.observe }

            val enum2 = groupBy2.groupedStats.getOrElse(0, null).asInstanceOf[EnumerationStat[jInt]]
            enum2.enumeration must haveSize(10)
            forall(10 until 20)(i => enum2.enumeration(i * 10) mustEqual 1L)

            groupBy += groupBy2
            val enum = groupBy.groupedStats.getOrElse(0, null).asInstanceOf[EnumerationStat[jInt]]
            enum.enumeration must haveSize(20)
            forall(0 until 20)(i => enum.enumeration(i * 10) mustEqual 1L)
            enum2.enumeration must haveSize(10)
            forall(10 until 20)(i => enum2.enumeration(i * 10) mustEqual 1L)
          }

          "clear" >> {
            val groupBy = newStat[Int]("cat1","Enumeration(intAttr)")
            groupBy.isEmpty must beFalse

            groupBy.clear()

            groupBy.isEmpty must beTrue
            groupBy.groupedStats.size mustEqual 0
          }
        }
      }

//      "TopK Stat and" >> {
//
//      }
//
//      "Frequency Stat and" >> {
//
//      }
//
//      "Z3Frequency Stat and" >> {
//
//      }
//
//      "Histogram Stat and" >> {
//
//      }
//
//      "Z3Histogram Stat and" >> {
//
//      }
//
//      "SeqStat Stat and" >> {
//        val seqStatStr = "MinMax(intAttr);IteratorStackCount();Enumeration(longAttr);Histogram(doubleAttr,20,0,200)"
//        "be empty initiallly" >> {
//          val groupBy = newStat[Int]("cat1", seqStatStr, false)
//          val seqStat = groupBy.groupedStats.getOrElse(0, null).asInstanceOf[SeqStat]
//          seqStat.stats must haveSize(4)
//          seqStat.isEmpty must beFalse
//
//          val mm = seqStat.stats(0).asInstanceOf[MinMax[jInt]]
//          val ic = seqStat.stats(1).asInstanceOf[IteratorStackCount]
//          val eh = seqStat.stats(2).asInstanceOf[EnumerationStat[jLong]]
//          val rh = seqStat.stats(3).asInstanceOf[Histogram[jDouble]]
//
//          mm.attribute mustEqual intIndex
//          mm.isEmpty must beTrue
//
//          ic.counter mustEqual 1
//
//          eh.attribute mustEqual longIndex
//          eh.enumeration must beEmpty
//
//          rh.attribute mustEqual doubleIndex
//          forall(0 until rh.length)(rh.count(_) mustEqual 0)
//        }

//        "observe correct values" >> {
//          val stat = newStat()
//
//          val stats = stat.stats
//
//          stats must haveSize(4)
//          stat.isEmpty must beFalse
//
//          val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
//          val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
//          val eh = stat.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
//          val rh = stat.stats(3).asInstanceOf[Histogram[java.lang.Double]]
//
//          mm.bounds mustEqual (0, 99)
//
//          ic.counter mustEqual 1
//
//          eh.enumeration.size mustEqual 100
//          eh.enumeration(0L) mustEqual 1
//          eh.enumeration(100L) mustEqual 0
//
//          rh.length mustEqual 20
//          rh.count(rh.indexOf(0.0)) mustEqual 10
//          rh.count(rh.indexOf(50.0)) mustEqual 10
//          rh.count(rh.indexOf(100.0)) mustEqual 0
//        }
//
//        "serialize to json" >> {
//          val stat = newStat()
//          stat.toJson must not(beEmpty)
//        }
//
//        "serialize empty to json" >> {
//          val stat = newStat(observe = false)
//          stat.toJson must not(beEmpty)
//        }
//
//        "serialize and deserialize" >> {
//          val stat = newStat()
//          val packed = StatSerializer(sft).serialize(stat)
//          val unpacked = StatSerializer(sft).deserialize(packed)
//          unpacked.toJson mustEqual stat.toJson
//        }
//
//        "serialize and deserialize empty SeqStat" >> {
//          val stat = newStat(observe = false)
//          val packed = StatSerializer(sft).serialize(stat)
//          val unpacked = StatSerializer(sft).deserialize(packed)
//          unpacked.toJson mustEqual stat.toJson
//        }
//
//        "deserialize as immutable value" >> {
//          val stat = newStat()
//          val packed = StatSerializer(sft).serialize(stat)
//          val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
//          unpacked.toJson mustEqual stat.toJson
//
//          unpacked.clear must throwAn[Exception]
//          unpacked.+=(stat) must throwAn[Exception]
//          unpacked.observe(features.head) must throwAn[Exception]
//          unpacked.unobserve(features.head) must throwAn[Exception]
//        }
//
//        "combine two SeqStats" >> {
//          val stat = newStat()
//          val stat2 = newStat(observe = false)
//
//          val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
//          val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
//          val eh = stat.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
//          val rh = stat.stats(3).asInstanceOf[Histogram[java.lang.Double]]
//
//          val mm2 = stat2.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
//          val ic2 = stat2.stats(1).asInstanceOf[IteratorStackCount]
//          val eh2 = stat2.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
//          val rh2 = stat2.stats(3).asInstanceOf[Histogram[java.lang.Double]]
//
//          ic2.counter mustEqual 1
//          mm2.isEmpty must beTrue
//          eh2.enumeration must beEmpty
//
//          rh2.length mustEqual 20
//          forall(0 until 20)(rh2.count(_) mustEqual 0)
//
//          features2.foreach { stat2.observe }
//
//          stat += stat2
//
//          mm.bounds mustEqual (0, 199)
//
//          ic.counter mustEqual 2
//
//          eh.enumeration.size mustEqual 200
//          eh.enumeration(0L) mustEqual 1
//          eh.enumeration(100L) mustEqual 1
//
//          rh.length mustEqual 20
//          rh.count(rh.indexOf(0.0)) mustEqual 10
//          rh.count(rh.indexOf(50.0)) mustEqual 10
//          rh.count(rh.indexOf(100.0)) mustEqual 10
//
//          mm2.bounds mustEqual (100, 199)
//
//          ic2.counter mustEqual 1
//
//          eh2.enumeration.size mustEqual 100
//          eh2.enumeration(0L) mustEqual 0
//          eh2.enumeration(100L) mustEqual 1
//
//          rh2.length mustEqual 20
//          rh2.count(rh2.indexOf(0.0)) mustEqual 0
//          rh2.count(rh2.indexOf(50.0)) mustEqual 0
//          rh2.count(rh2.indexOf(100.0)) mustEqual 10
//        }
//
//        "clear" >> {
//          val stat = newStat()
//          stat.isEmpty must beFalse
//
//          stat.clear()
//
//          val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
//          val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
//          val eh = stat.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
//          val rh = stat.stats(3).asInstanceOf[Histogram[java.lang.Double]]
//
//          mm.attribute mustEqual intIndex
//          mm.isEmpty must beTrue
//
//          ic.counter mustEqual 1
//
//          eh.attribute mustEqual longIndex
//          eh.enumeration must beEmpty
//
//          rh.attribute mustEqual doubleIndex
//          forall(0 until rh.length)(rh.count(_) mustEqual 0)
//        }
      }
    }
  }
}
