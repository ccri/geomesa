package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job

import scala.util.{Failure, Success, Try}

object AccumuloVersion extends Enumeration {
  type AccumuloVersion = Value
  val V15, V16, V17 = Value

  lazy val accumuloVersion: AccumuloVersion = {
    Try(classOf[AccumuloInputFormat].getMethod("addIterator", classOf[Job], classOf[IteratorSetting])) match {
      case Failure(t: NoSuchMethodException) => V15
      case Success(m)                        => V16
    }
  }

  lazy val AccumuloMetadataTableName = getMetadataTable
  lazy val AccumuloMetadataCF = getMetadataColumnFamily
  lazy val EmptyAuths = Authorizations.EMPTY

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
        getStringFromText("org.apache.accumulo.core.Constants", "METADATA_DATAFILE_COLUMN_FAMILY")
      case V16 =>
        getStringFromText("org.apache.accumulo.core.metadata.schema.MetadataSchema.TabletsSection.DataFileColumnFamily", "NAME")
    }
  }

  def getStringFromClass(className: String, field: String): String = {
    val clazz = Class.forName(className)
    clazz.getDeclaredField(field).get(classOf[String]).asInstanceOf[String]
  }

  def getStringFromText(className: String, field: String): Text = {
    val clazz = Class.forName(className)
    clazz.getDeclaredField(field).get(classOf[Text]).asInstanceOf[Text]
  }





}