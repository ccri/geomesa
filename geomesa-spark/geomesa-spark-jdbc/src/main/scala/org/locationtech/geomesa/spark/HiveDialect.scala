/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.sql.{Connection, ResultSet}
import java.util

import com.vividsolutions.jts.geom.{Envelope, Geometry, GeometryFactory, LinearRing, Point}
import com.vividsolutions.jts.io.WKTReader
import org.geotools.jdbc._
import org.geotools.geometry.jts.{CurvedRing, WKTReader2, WKTWriter2}
import org.opengis.feature.`type`.GeometryDescriptor


class HiveDialect(ds: JDBCDataStore) extends BasicSQLDialect(ds) {

  override def getNameEscape: String = ""

  override def isLimitOffsetSupported: Boolean = true

  override def applyLimitOffset(sql: StringBuffer, limit: Int, offset: Int): Unit = {
    ds.setFetchSize(limit)
    sql.append(" LIMIT " + limit)
  }

  override def registerSqlTypeNameToClassMappings(mappings: util.Map[String, Class[_]]): Unit = {
    super.registerSqlTypeNameToClassMappings(mappings)
    mappings.put("point", classOf[Point])
  }

  override def encodeGeometryValue(value: Geometry, dimension: Int, srid: Int, sql: StringBuffer): Unit = {
    if (value == null || value.isEmpty) sql.append("NULL")
    else {
      val output = if (value.isInstanceOf[LinearRing] && !value.isInstanceOf[CurvedRing]) {
        //postgis does not handle linear rings, convert to just a line string
        value.getFactory.createLineString(value.asInstanceOf[LinearRing].getCoordinateSequence)
      } else value
      val writer: WKTWriter2 = new WKTWriter2(dimension)
      val wkt: String = writer.write(output)
      sql.append("ST_GeomFromText('" + wkt + "', " + srid + ")")
    }
  }

  override def decodeGeometryEnvelope(rs: ResultSet, column: Int, cx: Connection): Envelope = {
    val reader: WKTReader2 = new WKTReader2()
    val spatialExtent: Array[String] = rs.getArray(column).asInstanceOf[Array[String]]
    val geoms: Array[Geometry] = spatialExtent.map(value => reader.read(value))
    geoms(0).getEnvelopeInternal
  }

  override def encodeGeometryEnvelope(tableName: String, geometryColumn: String, sql: StringBuffer): Unit = ???

  override def decodeGeometryValue(descriptor: GeometryDescriptor, rs: ResultSet, column: String,
                                   factory: GeometryFactory, cx: Connection): Geometry = {
    val wkt = rs.getString( column )
    if ( wkt == null ) {
         return null
       }
    new WKTReader(factory).read( wkt )
  }

  override def getDesiredTablesType: Array[String] = Array("TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM", "INDEX_TABLE")

}
