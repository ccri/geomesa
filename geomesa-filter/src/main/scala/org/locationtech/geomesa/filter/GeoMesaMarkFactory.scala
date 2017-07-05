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
import org.geotools.filter.LiteralExpressionImpl
import org.geotools.geometry.jts.TransformedShape
import org.geotools.renderer.style._
import org.opengis.feature.Feature
import org.opengis.filter.expression.Expression

import java.lang.{String => jString, Double => jDouble}

class GeoMesaMarkFactory extends TTFMarkFactory {
  // To use this factory utilize the 'geomesaFastMark' ogc function

   val cache = Caffeine.newBuilder().build[String, Array[Shape]](
      new CacheLoader[String, Array[Shape]] {
        override def load(key: String): Array[Shape] = {
          val notRotated = GeoMesaMarkFactory.lookupShape(key)
          if (notRotated != null) {
            Array.tabulate[Shape](360) { i =>
              val ts = new TransformedShape()
              ts.shape = notRotated
              ts.rotate(-i * Math.PI / 180)
              ts
            }
          } else {
            Array.tabulate[Shape](360) { null }
          }
        }
      }
   )

  override def getShape(graphics: Graphics2D, symbolUrl: Expression, feature: Feature): Shape = {
    // gm://$rotation#$iconPath i.e. gm://180.0#ttf://...

    symbolUrl.evaluate(feature, classOf[(jString, jDouble)]) match {
      case (icon: jString, rotation: jDouble) =>
        val normRotation: Int = {
          if (rotation < 0) {
            (rotation + 360).toInt
          } else {
            rotation.toInt
          }
        }
        require(normRotation >= 0 && normRotation <= 360)
        cache.get(icon)(normRotation)
      case _ => null
    }
//    if (url.startsWith("gm://")) {
//      val split = url.indexOf("#")
//      val iconUrl = url.substring(split + 1)
//      val rotation = {
//        val initial = java.lang.Double.parseDouble(url.substring(5, split)).toInt
//        if (initial < 0) {
//          initial + 360
//        } else {
//          initial
//        }
//      }
//      require(rotation >= 0 && rotation <= 360)
//
//      cache.get(iconUrl)(rotation)
//    } else {
//      null
//    }
  }
}

object GeoMesaMarkFactory {
  val ttfFactory = new TTFMarkFactory

  def lookupShape(url: String): Shape = {
    if (url.startsWith("ttf://")) {
      println(s"Looking up shape for url $url")
      val exp = new LiteralExpressionImpl(url)
      val shape = ttfFactory.getShape(null, exp, null)

      shape
    } else {
      null
    }
  }
}
