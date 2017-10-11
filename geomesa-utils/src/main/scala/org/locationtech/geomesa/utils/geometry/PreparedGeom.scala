/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geometry

import com.vividsolutions.jts.geom._
import com.vividsolutions.jts.geom.prep._

class PreparedGeom(geom: Geometry, factory: GeometryFactory) extends Geometry(factory) {

  def isPreparable: Boolean = geom match {
    case poly: Polygon => true
    case point: Point=> true
    case line: LineString => true
    case _ => false
  }

  val prepared: PreparedGeometry = geom match {
    case poly: Polygon => new PreparedPolygon(poly)
    case point: Point=> new PreparedPoint(point)
    case line: LineString => new PreparedLineString(line)
    case multi: MultiPolygon => new PreparedMultiPolygon(multi)
  }

  // Geometry Methods
  override def normalize(): Unit = geom.normalize()
  override def getDimension: Int = geom.getDimension
  override def compareToSameClass(o: scala.Any): Int = geom.compareTo(o)
  override def compareToSameClass(o: scala.Any, comp: CoordinateSequenceComparator): Int = geom.compareTo(o, comp)
  override def equalsExact(other: Geometry, tolerance: Double): Boolean = geom.equalsExact(other)
  override def getCoordinates: Array[Coordinate] = geom.getCoordinates
  override def computeEnvelopeInternal(): Envelope = geom.getEnvelopeInternal
  override def getNumPoints: Int = geom.getNumPoints
  override def apply(filter: CoordinateFilter): Unit = geom.apply(filter)
  override def apply(filter: CoordinateSequenceFilter): Unit = geom.apply(filter)
  override def apply(filter: GeometryFilter): Unit = geom.apply(filter)
  override def apply(filter: GeometryComponentFilter): Unit = geom.apply(filter)
  override def getBoundary: Geometry = geom.getBoundary
  override def getGeometryType: String = geom.getGeometryType
  override def isEmpty: Boolean = geom.isEmpty
  override def reverse(): Geometry = geom.reverse()
  override def getBoundaryDimension: Int = geom.getBoundaryDimension
  override def getCoordinate: Coordinate = geom.getCoordinate

  // Prepared Geometry Methods
  override def contains(g: Geometry): Boolean = prepared.contains(g)
  override def intersects(g: Geometry): Boolean = prepared.intersects(g)
  override def covers(g: Geometry): Boolean = prepared.covers(g)
  override def coveredBy(g: Geometry): Boolean = prepared.coveredBy(g)
  override def crosses(g: Geometry): Boolean = prepared.crosses(g)
  override def overlaps(g: Geometry): Boolean = prepared.overlaps(g)
  override def touches(g: Geometry): Boolean = prepared.touches(g)
  override def within(g: Geometry): Boolean = prepared.within(g)
  def getGeometry: Geometry = prepared.getGeometry


}
