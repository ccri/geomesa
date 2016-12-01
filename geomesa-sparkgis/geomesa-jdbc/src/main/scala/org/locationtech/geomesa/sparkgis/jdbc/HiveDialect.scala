package org.locationtech.geomesa.sparkgis.jdbc

import java.sql.{Connection, ResultSet}

import com.vividsolutions.jts.geom.{Envelope, Geometry, GeometryFactory, LinearRing}
import com.vividsolutions.jts.io.WKTWriter
import org.geotools.data.postgis.WKBAttributeIO
import org.geotools.geometry.jts.{CurvedRing, WKTWriter2}
import org.geotools.jdbc.{BasicSQLDialect, JDBCDataStore}
import org.opengis.feature.`type`.GeometryDescriptor

class HiveDialect(ds: JDBCDataStore) extends BasicSQLDialect(ds) {


  override def encodeGeometryValue(value: Geometry, dimension: Int, srid: Int, sql: StringBuffer): Unit = {
    if (value == null || value.isEmpty) sql.append("NULL")
    else {
      val output = if (value.isInstanceOf[LinearRing] && !value.isInstanceOf[CurvedRing]) {
        //postgis does not handle linear rings, convert to just a line string
        value.getFactory.createLineString(value.asInstanceOf[LinearRing].getCoordinateSequence)
      } else value
      val writer: WKTWriter = new WKTWriter2(dimension)
      val wkt: String = writer.write(output)
      sql.append("ST_GeomFromText('" + wkt + "', " + srid + ")")
    }
  }

  override def decodeGeometryEnvelope(rs: ResultSet, column: Int, cx: Connection): Envelope = {
    println("In decodeGeometry")
    ???
  }

  override def decodeGeometryValue(descriptor: GeometryDescriptor, rs: ResultSet, column: Int, factory: GeometryFactory, cx: Connection): Geometry = {
    val reader = getWKBReader(factory)
    reader.read(rs, column).asInstanceOf[Geometry]
  }
  override def encodeGeometryEnvelope(tableName: String, geometryColumn: String, sql: StringBuffer): Unit = ???

  val wkbReader = new ThreadLocal[WKBAttributeIO]

  private def getWKBReader(factory: GeometryFactory) = {
    var reader = wkbReader.get
    if (reader == null) {
      reader = new WKBAttributeIO(factory)
      wkbReader.set(reader)
    }
    else reader.setGeometryFactory(factory)
    reader
  }

  override def decodeGeometryValue(descriptor: GeometryDescriptor, rs: ResultSet, column: String, factory: GeometryFactory, cx: Connection): Geometry = ???

  override def getDesiredTablesType: Array[String] = Array("TABLE", "VIEW", "MATERIALIZED VIEW", "SYNONYM", "INDEX_TABLE")

  override def initializeConnection(cx: Connection): Unit = {
    val statement = cx.createStatement()
    statement.execute("show tables")
    val res = statement.getResultSet
    println(s"res: $res")

  }
}
