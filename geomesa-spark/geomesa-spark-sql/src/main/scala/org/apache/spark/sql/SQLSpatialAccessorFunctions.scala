/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom.{Point, LineString, Geometry}

object SQLSpatialAccessorFunctions {
  val ST_Boundary: Geometry => Geometry = geom => geom.getBoundary
  val ST_CoordDim: Geometry => Int = geom => geom.getDimension
  val ST_Dimension: Geometry => Int = geom => geom.getBoundaryDimension
  val ST_Envelope: Geometry => Geometry = geom => geom.getEnvelope
  val ST_ExteriorRing: Geometry => LineString = ???
  val ST_GeometryN: (Geometry, Int) => Geometry = (geom, n) => geom.getGeometryN(n)
  val ST_InteriorRingN: (Geometry, Int) => Geometry = ???
  val ST_IsClosed: Geometry => Boolean = ???
  val ST_IsCollection: Geometry => Boolean = ???
  val ST_IsEmpty: Geometry => Boolean = ???
  val ST_IsRing: Geometry => Boolean = ???
  val ST_IsSimple: Geometry => Boolean = ???
  val ST_IsValid: Geometry => Boolean = ???
  val ST_NumGeometries: Geometry => Int = ???
  val ST_NumPoints: Geometry => Int = ???
  val ST_PointN: (Geometry, Int) => Point = ???
  val ST_X: Point => Float = ???
  val ST_Y: Point => Float = ???
}
