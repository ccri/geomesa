/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geometry

import com.vividsolutions.jts.geom.{Geometry, MultiPolygon, Polygon}
import com.vividsolutions.jts.geom.prep.{PreparedGeometry, PreparedPolygon}

class PreparedMultiPolygon(multiPoly: MultiPolygon) extends PreparedGeometry {

  val prepared: Seq[PreparedPolygon] = {
    for (n <- 0 until multiPoly.getNumGeometries)
      yield new PreparedPolygon(multiPoly.getGeometryN(n).asInstanceOf[Polygon])
  }

  override def intersects(geom: Geometry): Boolean = prepared.indexWhere(p => p.intersects(geom)) != -1

  override def within(geom: Geometry): Boolean = prepared.indexWhere(p => p.within(geom)) != -1

  override def coveredBy(geom: Geometry): Boolean = prepared.indexWhere(p => p.coveredBy(geom)) != -1

  override def getGeometry: Geometry = multiPoly

  override def crosses(geom: Geometry): Boolean = prepared.indexWhere(p => p.crosses(geom)) != -1

  override def touches(geom: Geometry): Boolean = prepared.indexWhere(p => p.touches(geom)) != -1

  override def contains(geom: Geometry): Boolean = prepared.indexWhere(p => p.contains(geom)) != -1

  override def containsProperly(geom: Geometry): Boolean = prepared.indexWhere(p => p.containsProperly(geom)) != -1

  override def disjoint(geom: Geometry): Boolean = prepared.indexWhere(p => p.disjoint(geom)) != -1

  override def overlaps(geom: Geometry): Boolean = prepared.indexWhere(p => p.overlaps(geom)) != -1

  override def covers(geom: Geometry): Boolean = prepared.indexWhere(p => p.covers(geom)) != -1
}
