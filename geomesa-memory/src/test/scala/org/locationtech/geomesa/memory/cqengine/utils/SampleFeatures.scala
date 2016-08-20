/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import com.google.common.base.Ticker
import com.googlecode.cqengine.query.option.QueryOptions
import com.googlecode.cqengine.query.{Query, QueryFactory => CQF}
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.filter.visitor.SimplifyingFilterVisitor
import org.joda.time.{DateTime, DateTimeZone, Instant}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.memory.cqengine.index.GeoIndex
import org.locationtech.geomesa.memory.cqengine.query
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language._
import scala.util.Random

object SampleFeatures {
  val spec = "Who:String:index=full,What:Integer,When:Date,*Where:Point:srid=4326,Why:String"
  val sft = SimpleFeatureTypes.createType("test", spec)
  val cq = SFTAttributes(sft)
  val ff = CommonFactoryFinder.getFilterFactory2

  val MIN_DATE = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeZone.forID("UTC"))
  val seconds_per_year = 365L * 24L * 60L * 60L
  val string = "foo"

  def randDate = MIN_DATE.plusSeconds(scala.math.round(scala.util.Random.nextFloat * seconds_per_year)).toDate

  val builder = new SimpleFeatureBuilder(sft)

  val names = Array("Addams", "Bierce", "Clemens", "Damon", "Evan", "Fred", "Goliath", "Harry")

  def getName: String = names(Random.nextInt(names.length))

  def getPoint: Point = {
    val minx = -180
    val miny = -90
    val dx = 360
    val dy = 180

    val x = minx + Random.nextDouble * dx
    val y = miny + Random.nextDouble * dy
    WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
  }

  def buildFeature(i: Int): SimpleFeature = {
    builder.set("Who", getName)
    builder.set("What", Random.nextInt(10))
    builder.set("When", randDate)
    builder.set("Where", getPoint)
    if (Random.nextBoolean()) {
      builder.set("Why", string)
    }
    builder.buildFeature(i.toString)
  }

}
