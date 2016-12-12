/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import java.awt.geom.AffineTransform

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.udaf.ConvexHull
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.referencing.operation.transform.AffineTransform2D
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}

object SQLSpatialFunctions {
  // Geometry constructors

  // TODO: optimize when used as a literal
  // e.g. select * from feature where st_contains(geom, geomFromText('POLYGON((....))'))
  // should not deserialize the POLYGON for every call
  val ST_Box2DFromGeoHash: (String, Int) => Geometry = (hash, prec) => ST_GeomFromGeoHash(hash, prec)
  val ST_GeomFromGeoHash: (String, Int) => Geometry = (hash, prec) => GeoHash(hash, prec).geom
  val ST_GeomFromWKT: String => Geometry = text => WKTUtils.read(text)
  val ST_GeomFromWKB: Array[Byte] => Geometry = array => WKBUtils.read(array)
  val ST_MakeBox2D: (Point, Point) => Polygon = (ll, ur) =>
    JTS.toGeometry(new Envelope(ll.getX, ur.getX, ll.getY, ur.getY))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = (lx, ly, ux, uy) =>
    JTS.toGeometry(BoundingBox(lx, ux, ly, uy))
  val ST_MakePolygon: LineString => Polygon = shell => {
    require(shell.isClosed)
    val envelope = JTS.toEnvelope(new LinearRing(shell.getCoordinateSequence, SQLTypes.geomFactory).getEnvelope)
    JTS.toGeometry(envelope)
  }
  val ST_MakePoint: (Double, Double) => Point = (x, y) => WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
  val ST_MakePointM: (Double, Double, Double) => Point = (x, y, m) =>
    WKTUtils.read(s"POINT($x $y $m)").asInstanceOf[Point]
  val ST_MLineFromText: String => MultiLineString = (text) => WKTUtils.read(text).asInstanceOf[MultiLineString]
  val ST_MPointFromText: String => MultiPoint = (text) => WKTUtils.read(text).asInstanceOf[MultiPoint]
  val ST_MPolyFromText: String => MultiPolygon = (text) => WKTUtils.read(text).asInstanceOf[MultiPolygon]
  val ST_Point: (Double, Double) => Point = (x, y) => ST_MakePoint(x, y)
  val ST_PointFromGeoHash: (String, Int) => Point = (hash, prec) => GeoHash(hash, prec).getPoint
  val ST_PointFromText: String => Point = text => WKTUtils.read(text).asInstanceOf[Point]
  val ST_PointFromWKB: Array[Byte] => Point = array => ST_GeomFromWKB(array).asInstanceOf[Point]
  val ST_Polygon: LineString => Polygon = shell => ST_MakePolygon(shell)
  val ST_PolygonFromText: String => Polygon = text => WKTUtils.read(text).asInstanceOf[Polygon]
  // Geometry accessors
  val ST_Envelope:  Geometry => Geometry = p => p.getEnvelope

  // Geometry editors
  val ST_Translate: (Geometry, Double, Double) => Geometry =
    (g, deltaX, deltaY) => translate(g, deltaX, deltaY)

  // Spatial relationships
  val ST_Contains:   (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.contains(geom2)
  val ST_Covers:     (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.covers(geom2)
  val ST_Crosses:    (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.crosses(geom2)
  val ST_Disjoint:   (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.disjoint(geom2)
  val ST_Equals:     (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.equals(geom2)
  val ST_Intersects: (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.intersects(geom2)
  val ST_Overlaps:   (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.overlaps(geom2)
  val ST_Touches:    (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.touches(geom2)
  val ST_Within:     (Geometry, Geometry) => Boolean = (geom1, geom2) => geom1.within(geom2)

  val ST_Centroid: Geometry => Point = g => g.getCentroid
  val ST_DistanceSpheroid: (Geometry, Geometry) => java.lang.Double = (s, e) => fastDistance(s, e)

  // Geometry Processing
  val ch = new ConvexHull

  // Type casting functions
  // TODO: Implement addition casts
  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]
  val ST_ByteArray: (String) => Array[Byte] = (string) => string.getBytes

  def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register geometry constructors
    sqlContext.udf.register("st_box2DFromGeoHash"  , ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromGeoHash"   , ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromWKT"       , ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText"  , ST_GeomFromWKT)
    sqlContext.udf.register("st_geomFromWKB"       , ST_GeomFromWKB)
    sqlContext.udf.register("st_makeBox2D"         , ST_MakeBox2D)
    sqlContext.udf.register("st_makeBBOX"          , ST_MakeBBOX)
    sqlContext.udf.register("st_makePolygon"       , ST_MakePolygon)
    sqlContext.udf.register("st_makePoint"         , ST_MakePoint)
    sqlContext.udf.register("st_makePointM"        , ST_MakePointM)
    sqlContext.udf.register("st_mLineFromText"     , ST_MLineFromText)
    sqlContext.udf.register("st_mPointFromText"    , ST_MPointFromText)
    sqlContext.udf.register("st_mPolyFromText"     , ST_MPolyFromText)
    sqlContext.udf.register("ST_point"             , ST_Point)
    sqlContext.udf.register("ST_pointFromGeoHash"  , ST_PointFromGeoHash)
    sqlContext.udf.register("st_pointFromText"     , ST_PointFromText)
    sqlContext.udf.register("st_pointFromWKB"      , ST_PointFromWKB)
    sqlContext.udf.register("st_polygon"           , ST_Polygon)
    sqlContext.udf.register("st_polygonFromText"   , ST_PolygonFromText)

    // Register geometry accessors
    sqlContext.udf.register("st_envelope"      , ST_Envelope)

    // Register geometry editors
    sqlContext.udf.register("st_translate", ST_Translate)

    // Register spatial relationships
    sqlContext.udf.register("st_contains"    , ST_Contains)
    sqlContext.udf.register("st_covers"      , ST_Covers)
    sqlContext.udf.register("st_crosses"     , ST_Crosses)
    sqlContext.udf.register("st_disjoint"    , ST_Disjoint)
    sqlContext.udf.register("st_equals"      , ST_Equals)
    sqlContext.udf.register("st_intersects"  , ST_Intersects)
    sqlContext.udf.register("st_overlaps"    , ST_Overlaps)
    sqlContext.udf.register("st_touches"     , ST_Touches)
    sqlContext.udf.register("st_within"      , ST_Within)

    sqlContext.udf.register("st_centroid"      , ST_Centroid)
    sqlContext.udf.register("st_distanceSpheroid"  , ST_DistanceSpheroid)

    // Register geometry Processing
    sqlContext.udf.register("st_convexhull", ch)

    // Register type casting functions
    sqlContext.udf.register("st_castToPoint", ST_CastToPoint)
    sqlContext.udf.register("st_castToPolygon", ST_CastToPolygon)
    sqlContext.udf.register("st_castToLineString", ST_CastToLineString)
    sqlContext.udf.register("st_byteArray", ST_ByteArray)
  }

  @transient private val geoCalcs = new ThreadLocal[GeodeticCalculator] {
    override def initialValue(): GeodeticCalculator = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
  }

  def fastDistance(s: Geometry, e: Geometry): Double = {
    val calc = geoCalcs.get()
    val c1 = s.getCentroid.getCoordinate
    calc.setStartingGeographicPoint(c1.x, c1.y)
    val c2 = e.getCentroid.getCoordinate
    calc.setDestinationGeographicPoint(c2.x, c2.y)
    calc.getOrthodromicDistance
  }

  def translate(g: Geometry, deltax: Double, deltay: Double): Geometry = {
    val affineTransform = AffineTransform.getTranslateInstance(deltax, deltay)
    val transform = new AffineTransform2D(affineTransform)
    JTS.transform(g, transform)
  }
}
