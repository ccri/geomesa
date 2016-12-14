package org.apache.spark.sql

import com.vividsolutions.jts.geom.Geometry

object SQLSpatialAccessorFunctions {
  val ST_Boundary: Geometry => Geometry = geom => geom.getBoundary
  val ST_CoordDim: Geometry => Int = geom => {
    val coord = geom.getCoordinate
    if (coord.z.isNaN) 2 else 3
  }
  val ST_Dimension: Geometry => Int = geom => geom.getDimension
  val ST_Envelope: Geometry => Geometry = geom => geom.getEnvelope
  //  val ST_ExteriorRing: Geometry => LineString = ???
  val ST_GeometryN: (Geometry, Int) => Geometry = (geom, n) => geom.getGeometryN(n)
  //  val ST_InteriorRingN: (Geometry, Int) => Geometry = ???
  //  val ST_IsClosed: Geometry => Boolean = ???
  //  val ST_IsCollection: Geometry => Boolean = ???
  //  val ST_IsEmpty: Geometry => Boolean = ???
  //  val ST_IsRing: Geometry => Boolean = ???
  //  val ST_IsSimple: Geometry => Boolean = ???
  //  val ST_IsValid: Geometry => Boolean = ???
  //  val ST_NumGeometries: Geometry => Int = ???
  //  val ST_NumPoints: Geometry => Int = ???
  //  val ST_PointN: (Geometry, Int) => Point = ???
  //  val ST_X: Point => Float = ???
  //  val ST_Y: Point => Float = ???
}
