package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.impl.{Tables, MasterClient}
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.security.thrift.TCredentials
import org.apache.accumulo.trace.instrument.Tracer
import org.apache.hadoop.io.Text

import scala.reflect.ClassTag

object AccumuloVersion extends Enumeration {
  type AccumuloVersion = Value
  val V15, V16, V17 = Value

  lazy val accumuloVersion: AccumuloVersion = {
    if      (Constants.VERSION.startsWith("1.5")) V15
    else if (Constants.VERSION.startsWith("1.6")) V16
    else if (Constants.VERSION.startsWith("1.7")) V17
    else {
      throw new Exception(s"GeoMesa does not currently support Accumulo ${Constants.VERSION}.")
    }
  }

  lazy val AccumuloMetadataTableName = getMetadataTable
  lazy val AccumuloMetadataCF = getMetadataColumnFamily
  lazy val EmptyAuths: Authorizations = getEmptyAuths

  def getMetadataTable: String = {
    accumuloVersion match {
      case V15 =>
        getTypeFromClass("org.apache.accumulo.core.Constants", "METADATA_TABLE_NAME")
      case V16 =>
        getTypeFromClass("org.apache.accumulo.core.metadata.MetadataTable", "NAME")
      case V17 =>
        getTypeFromClass("org.apache.accumulo.core.metadata.MetadataTable", "NAME")
      case _ =>
        getTypeFromClass("org.apache.accumulo.core.metadata.MetadataTable", "NAME")
    }
  }

  def getMetadataColumnFamily: Text = {
    accumuloVersion match {
      case V15 =>
        getTypeFromClass("org.apache.accumulo.core.Constants", "METADATA_DATAFILE_COLUMN_FAMILY")
      case V16 =>
        getTypeFromClass("org.apache.accumulo.core.metadata.schema.MetadataSchema$TabletsSection$DataFileColumnFamily", "NAME")
      case V17 =>
        getTypeFromClass("org.apache.accumulo.core.metadata.schema.MetadataSchema$TabletsSection$DataFileColumnFamily", "NAME")
      case _ =>
        getTypeFromClass("org.apache.accumulo.core.metadata.schema.MetadataSchema$TabletsSection$DataFileColumnFamily", "NAME")
    }
  }

  def getEmptyAuths: Authorizations = {
    accumuloVersion match {
      case V15 =>
        getTypeFromClass("org.apache.accumulo.core.Constants", "NO_AUTHS")
      case V16 =>
        getTypeFromClass("org.apache.accumulo.core.security.Authorizations", "EMPTY")
      case V17 =>
        getTypeFromClass("org.apache.accumulo.core.security.Authorizations", "EMPTY")
      case _ =>
        getTypeFromClass("org.apache.accumulo.core.security.Authorizations", "EMPTY")
    }
  }

  def getTypeFromClass[T: ClassTag](className: String, field: String): T = {
    val clazz = Class.forName(className)
    clazz.getDeclaredField(field).get(null).asInstanceOf[T]
  }

  def calculateTableSize(connector: Connector, tableName: String): Long = {
    accumuloVersion match {
      case V15 => getTableSize15(connector, tableName)
      case V16 => getTableSize16(connector, tableName)
      case V17 => getTableSize17(connector, tableName)
      case _   => getTableSize17(connector, tableName)
    }
  }

  // TODO: Convert to reflection and sort out things.
  def getTableSize15(connector: Connector, tableName: String): Long = {
    if (connector.isInstanceOf[MockConnector]) {
      -1
    } else {
      val masterClient = MasterClient.getConnection(connector.getInstance())
      val tc = new TCredentials()
      val mmi = masterClient.getMasterStats(Tracer.traceInfo(), tc)

      val tableId = Tables.getTableId(connector.getInstance(), tableName)
      val v = mmi.getTableMap.get(tableId)
      v.getRecs
    }
  }

  def getTableSize16(connector: Connector, tableName: String): Long = {
    getTableSize15(connector, tableName)
  }

  //TODO implement for Accumulo 1.7.
  def getTableSize17(connector: Connector, tableName: String): Long = {
    -1
  }
}