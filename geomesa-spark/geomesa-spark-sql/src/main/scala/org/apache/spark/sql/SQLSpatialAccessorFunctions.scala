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
  val ST_GeometryN: (Geometry, Int) => Geometry = ???
  val ST_InteriorRingN: Geometry = ???
  val ST_IsClosed: Boolean = ???
  val ST_IsCollection: Boolean = ???
  val ST_IsEmpty: Boolean = ???
  val ST_IsRing: Boolean = ???
  val ST_IsSimple: Boolean = ???
  val ST_IsValid: Boolean = ???
  val ST_NumGeometries: Int = ???
  val ST_NumPoints: Int = ???
  val ST_PointN: Point = ???
  val ST_X: Int = ???
  val ST_Y: Int = ???
}
