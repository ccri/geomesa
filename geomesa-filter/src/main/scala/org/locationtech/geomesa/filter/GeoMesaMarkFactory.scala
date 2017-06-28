/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import java.awt.{Graphics2D, Shape}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.geotools.filter.{AttributeExpressionImpl, LiteralExpressionImpl}
import org.geotools.geometry.jts.TransformedShape
import org.geotools.renderer.style.{MarkFactory, _}
import org.opengis.feature.Feature
import org.opengis.filter.expression.Expression

class GeoMesaMarkFactory extends MarkFactory  {

   val cache = Caffeine.newBuilder().build[String, Array[Shape]](
      new CacheLoader[String, Array[Shape]] {
        override def load(key: String): Array[Shape] = {
          val notRotated = GeoMesaMarkFactory.lookupShape(key)
           Array.tabulate[Shape](360){ i =>
             val ts = new TransformedShape()
             ts.shape = notRotated
             ts.rotate(-i * Math.PI / 180)
             ts
           }
        }
      }
   )

//  val cache = Caffeine.newBuilder().build[(String, Double), Shape](
//    new CacheLoader[(String, Double), Shape]() {
//      override def load(key: (String, Double)): Shape = GeoMesaMarkFactory.lookupShape(key)
//    }
//  )

  def getShape(graphics: Graphics2D, symbolUrl: Expression, feature: Feature): Shape = {

    val url = symbolUrl.evaluate(feature, classOf[String])

    if (url.contains("ttf://") && !url.startsWith("ttf")) {
      val indexT = url.indexOf("t")
      val iconUrl = url.substring(indexT)

      val rotation = {
        val initial = java.lang.Double.parseDouble(url.substring(0, indexT)).toInt
        if (initial < 0) {
          initial + 360
        } else {
          initial
        }
      }
      require(rotation >= 0 && rotation <= 360)

      cache.get(iconUrl)(rotation)
    } else {
      null
    }
  }
}

object GeoMesaMarkFactory {
  val ttfFactory = new TTFMarkFactory


  def lookupShape(url: String): Shape = {
    println(s"Looking up shape for url $url")
    val exp = new LiteralExpressionImpl(url)
    val shape = ttfFactory.getShape(null, exp, null)

    shape
  }
}
