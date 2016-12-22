/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.utils.text.WKBUtils

import scala.collection.mutable.ListBuffer


object SQLGeometricOutputFunctions {
  val ST_AsBinary: Geometry => Array[Byte] = geom => WKBUtils.write(geom)
  //val ST_AsGeoJSON: Geometry => String = geom => toGeoJson(geom)
  val ST_AsLatLonText: Geometry => String = geom => toLatLonString(geom)
  val ST_AsText: Geometry => String = geom => geom.toText
  // val ST_GeoHash


  def toLatLonString(g: Geometry): String = {
    val coordinate = g.getCoordinate
    s"${latLonFormat(coordinate.x, lat = false)} ${latLonFormat(coordinate.y, lat = true)}"
  }

  def latLonFormat(value: Double, lat: Boolean): String = {
    val degrees = value.floor
    val decimal = value - degrees
    val minutes = (decimal * 60).floor
    val seconds = (decimal * 60 - minutes) * 60
    if (lat)
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "S" else "N"}"
     else
      f"${degrees.abs}%1.0f\u00B0$minutes%1.0f" +"\'" + f"$seconds%1.3f" + "\"" + s"${if (degrees < 0) "W" else "E"}"
  }

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_asBinary", ST_AsBinary)
    //sqlContext.udf.register("st_asGeoJSON", ST_AsGeoJSON)
    sqlContext.udf.register("st_asLatLonText", ST_AsLatLonText)
    sqlContext.udf.register("st_asText", ST_AsText)
    //sqlContext.udf.register("st_geoHash", ST_GeoHash)

  }

  /*def toGeoJson(geom: Geometry): String = {
    val geoString = geom.toText
    val parenthesis = geoString.indexOf('(')
    val geomType = geoString.substring(0, parenthesis).trim
    val formattedCoords = formatCoordinates(geoString.substring(parenthesis).trim)
    "{\"type\":\"" + s"$geomType" + "\",\"coordinates\":" + s"$formattedCoords}"
  }*/

 /* def formatCoordinates(string: String): String = {
    var formattedString = "["
    var queue = new ListBuffer[String]()
    for (char <- string) {
      if (char == '(') formattedString += "["
      else if (char == ')') {
        formattedString += queue.mkString(",")
        queue.clear()
        formattedString += "]"
      }
      else if (char == ',') {
        formattedString += queue.mkString(",")
        queue.clear()
        formattedString += "],["
      }
      else if (char == ' ') formattedString += ""
      else {
        queue += char.toString
      }
    }
    formattedString += "]"
    formattedString
  }*/
}
