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
      "nested GroupBy Count() and" >> {
        "be empty initially" >> {
          val groupBy = newStat[Int]("cat1","GroupBy(cat2,Count())", false)
          groupBy.toJson mustEqual "[]"
          groupBy.isEmpty must beTrue
        }

        "observe correct values" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          groupBy.groupedStats.size mustEqual 10
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 100L
        }

        "unobserve correct values" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          groupBy.groupedStats.size mustEqual 10
          features.take(10).foreach(groupBy.unobserve)
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 90L
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat2", "GroupBy(cat1,Count())")
          groupBy.toJson mustEqual """[{ "8" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "2" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "5" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "4" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "7" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "1" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "9" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "3" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "6" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "0" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]}]"""
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          groupBy.toJson mustEqual """[{ "8" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "2" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "5" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "4" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "7" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "1" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "9" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "3" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "6" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]},{ "0" : [{ "S" : { "count": 100 }},{ "M" : { "count": 100 }},{ "V" : { "count": 100 }},{ "D" : { "count": 100 }},{ "P" : { "count": 100 }},{ "Y" : { "count": 100 }},{ "G" : { "count": 100 }},{ "J" : { "count": 100 }},{ "A" : { "count": 100 }},{ "R" : { "count": 100 }},{ "I" : { "count": 100 }},{ "C" : { "count": 100 }},{ "U" : { "count": 100 }},{ "L" : { "count": 100 }},{ "O" : { "count": 100 }},{ "F" : { "count": 100 }},{ "X" : { "count": 100 }},{ "W" : { "count": 100 }},{ "E" : { "count": 100 }},{ "N" : { "count": 100 }},{ "Z" : { "count": 100 }},{ "H" : { "count": 100 }},{ "Q" : { "count": 100 }},{ "T" : { "count": 100 }},{ "K" : { "count": 100 }},{ "B" : { "count": 100 }}]}]"""
        }

        "serialize and deserialize" >> {
          "observed" >> {
            val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            groupBy.toJson mustEqual unpacked.toJson
          }
          "unobserved" >> {
            val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())", false)
            val packed = StatSerializer(sft).serialize(groupBy)
            val unpacked = StatSerializer(sft).deserialize(packed)
            groupBy.toJson mustEqual unpacked.toJson
          }
        }

        "deserialize as immutable value" >> {
          val groupBy = newStat[Int]("cat1", "GroupBy(cat2,Count())")
          val packed = StatSerializer(sft).serialize(groupBy)
          val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
          unpacked.toJson mustEqual groupBy.toJson

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
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 100L
        }

        "unobserve correct values" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          groupBy.groupedStats.size mustEqual 10
          features.take(10).foreach(groupBy.unobserve)
          groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[CountStat].counter mustEqual 90L
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat1", "Count()")
          groupBy.toJson mustEqual """[{ "8" : { "count": 100 }},{ "2" : { "count": 100 }},{ "5" : { "count": 100 }},{ "4" : { "count": 100 }},{ "7" : { "count": 100 }},{ "1" : { "count": 100 }},{ "9" : { "count": 100 }},{ "3" : { "count": 100 }},{ "6" : { "count": 100 }},{ "0" : { "count": 100 }}]"""
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
          val groupedStats0 = groupBy.groupedStats.get(0).getOrElse(null).asInstanceOf[MinMax[String]]
          groupedStats0.bounds mustEqual ("abc000", "abc099")
          groupedStats0.cardinality must beCloseTo(100L, 5)
        }

        "serialize to json" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)")
          groupBy.toJson mustEqual """[{ "8" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "2" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "5" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "4" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "7" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "1" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "9" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "3" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "6" : { "min": "abc000", "max": "abc099", "cardinality": 104 }},{ "0" : { "min": "abc000", "max": "abc099", "cardinality": 104 }}]"""
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
          gS20.bounds mustEqual ("abc100", "abc199")
          gS20.cardinality must beCloseTo(100L, 5)

          groupBy1 += groupBy2
          val gS10 = groupBy1.groupedStats.get(0).getOrElse(null).asInstanceOf[MinMax[String]]
          gS10.bounds mustEqual ("abc000", "abc199")
          gS10.cardinality must beCloseTo(200L, 5)
          gS20.bounds mustEqual ("abc100", "abc199")
        }

        "clear" >> {
          val groupBy = newStat[Int]("cat1","MinMax(strAttr)")
          groupBy.isEmpty must beFalse

          groupBy.clear()

          groupBy.isEmpty must beTrue
          groupBy.groupedStats.size mustEqual 0
        }
      }
    }
  }
}
