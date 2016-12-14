package org.apache.spark.sql

import com.vividsolutions.jts.geom.{Polygon, Geometry}

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
  val ST_InteriorRingN: (Geometry, Int) => Geometry = (geom, int) => {
    geom match {
      case geom: Polygon => {
        if (0 < int && int <= geom.getNumInteriorRing) {
          geom.getInteriorRingN(int-1)
        } else {
          null
        }
      }
      case _ => null
    }
  }
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
