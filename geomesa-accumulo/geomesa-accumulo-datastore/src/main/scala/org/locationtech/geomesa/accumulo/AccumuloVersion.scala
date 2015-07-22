package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.Constants
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text

object AccumuloVersion extends Enumeration {
  type AccumuloVersion = Value
  val V15, V16, V17 = Value

  lazy val accumuloVersion: AccumuloVersion = {
    if      (Constants.VERSION.startsWith("1.5")) { announceVersion("1.5"); V15 }
    else if (Constants.VERSION.startsWith("1.6")) { announceVersion("1.6"); V16 }
    else if (Constants.VERSION.startsWith("1.7")) { announceVersion("1.7"); V17 }
    else {
      throw new Exception(s"GeoMesa does not currently support Accumulo ${Constants.VERSION}.")
    }
  }
  def announceVersion(v: String) = {
    println("*****************")
    println(s"VERSION: $v detected")
    println("*****************")
  }

  lazy val AccumuloMetadataTableName = printVal("AccumuloMetaTableName", getMetadataTable)
  lazy val AccumuloMetadataCF = printVal("MetaCF", getMetadataColumnFamily)
  lazy val EmptyAuths: Authorizations = printVal("Empty", getEmptyAuths)

  def printVal[T](s: String, v: T): T = {println(s"***$s: $v"); v}

  def getMetadataTable: String = {
    accumuloVersion match {
      case V15 =>
        getStringFromClass("org.apache.accumulo.core.Constants", "METADATA_TABLE_NAME")
      case V16 =>
        getStringFromClass("org.apache.accumulo.core.metadata.MetadataTable", "NAME")
    }
  }

  def getMetadataColumnFamily: Text = {
    accumuloVersion match {
      case V15 =>
        getTextFromString("org.apache.accumulo.core.Constants", "METADATA_DATAFILE_COLUMN_FAMILY")
      case V16 =>
        getTextFromString("org.apache.accumulo.core.metadata.schema.MetadataSchema$TabletsSection$DataFileColumnFamily", "NAME")
    }
  }

  def getEmptyAuths: Authorizations = {
    accumuloVersion match {
      case V15 =>
        getAuthsFromClass("org.apache.accumulo.core.Constants", "NO_AUTHS")
      case V16 =>
        getAuthsFromClass("org.apache.accumulo.core.security.Authorizations", "EMPTY")
    }
  }


  def getStringFromClass(className: String, field: String): String = {
    val clazz = Class.forName(className)
    clazz.getDeclaredField(field).get(classOf[String]).asInstanceOf[String]
  }

  def getTextFromString(className: String, field: String): Text = {
    val clazz = Class.forName(className)
    clazz.getDeclaredField(field).get(classOf[Text]).asInstanceOf[Text]
  }

  def getAuthsFromClass(className: String, field: String): Authorizations = {
    val clazz = Class.forName(className)
    clazz.getDeclaredField(field).get(classOf[Text]).asInstanceOf[Authorizations]
  }





}