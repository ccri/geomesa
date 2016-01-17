/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.core

import java.io.{File, FileInputStream}
import java.nio.channels.FileChannel
import java.util.{Map => JMap, Date}

import com.google.common.io.{ByteStreams, Files}
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{Query, Transaction}
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, _}
import org.locationtech.geomesa.accumulo.util.{GeoMesaBatchWriterConfig, SelfClosingIterator}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.blob.core.handlers._
import org.locationtech.geomesa.utils.filters.Filters
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

class AccumuloBlobStore(ds: AccumuloDataStore) extends LazyLogging with BlobStoreFileName {

  private val connector = ds.connector
  private val tableOps = connector.tableOperations()

  val blobTableName = s"${ds.catalogTable}_blob"

  AccumuloVersion.ensureTableExists(connector, blobTableName)
  ds.createSchema(sft)
  val bwc = GeoMesaBatchWriterConfig()
  val bw = connector.createBatchWriter(blobTableName, bwc)
  val fs = ds.getFeatureSource(blobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  def put(file: File, params: JMap[String, String]): Option[String] = {
    put(file, mapAsScalaMapConverter(params).asScala.toMap)
  }

  def put(file: File, params: scala.collection.immutable.Map[String, String]): Option[String] = {
    BlobStoreFileHandler.buildSF(file, params).map {
      sf =>
        val id = sf.getAttribute(idFieldName).asInstanceOf[String]

        fs.addFeatures(new ListFeatureCollection(sft, List(sf).asJava))
        putInternal(file, id, params)
        id
    }
  }

  def put(fis: FileInputStream, params: JMap[String, String]): String = {
    val sf = BlobStoreFileInputStreamHandler.buildSF(params)
    val id = sf.getAttribute(idFieldName).asInstanceOf[String]
    val fileName = sf.getAttribute(filenameFieldName).asInstanceOf[String]
    fs.addFeatures(new ListFeatureCollection(sft, List(sf).asJava))
    putInternal(fis, id, fileName)
    id
  }

  def put(fis: FileInputStream, fileName: String, geometry: Geometry, dtg: Date): String = {
    val sf = BlobStoreFileInputStreamHandler.buildBlobSF(fileName, geometry, dtg)
    val id = sf.getAttribute(idFieldName).asInstanceOf[String]
    fs.addFeatures(new ListFeatureCollection(sft, List(sf).asJava))
    putInternal(fis, id, fileName)
    id
  }

  def getIds(filter: Filter): java.util.Iterator[String] = {
    getIds(new Query(blobFeatureTypeName, filter))
  }

  def getIds(query: Query): java.util.Iterator[String] = {
    fs.getFeatures(query).features.map(_.getAttribute(idFieldName).asInstanceOf[String]).asJava
  }

  def get(id: String): (Array[Byte], String) = {
    // TODO: Get Authorizations using AuthorizationsProvider interface
    // https://geomesa.atlassian.net/browse/GEOMESA-986
    val scanner = connector.createScanner(blobTableName, new Authorizations())
    scanner.setRange(new Range(new Text(id)))

    val iter = SelfClosingIterator(scanner)
    if (iter.hasNext) {
      val ret = buildReturn(iter.next)
      iter.close()
      ret
    } else {
      (Array.empty[Byte], "")
    }
  }

  def delete(id: String): Unit = {
    // TODO: Get Authorizations using AuthorizationsProvider interface
    // https://geomesa.atlassian.net/browse/GEOMESA-986
    val bd = connector.createBatchDeleter(blobTableName, new Authorizations(), bwc.getMaxWriteThreads, bwc)
    bd.setRanges(List(new Range(new Text(id))).asJava)
    bd.delete()
    bd.close()
    deleteFeature(id)
  }

  private def deleteFeature(id: String): Unit = {
    val removalFilter = Filters.ff.id(Filters.ff.featureId(id))
    val fd = ds.getFeatureWriter(blobFeatureTypeName, removalFilter, Transaction.AUTO_COMMIT)
    try {
      fd.remove()
    } catch {
      case e: Exception =>
        logger.error(e.getMessage)
    } finally {
      fd.close()
    }
  }

  private def buildReturn(entry: java.util.Map.Entry[Key, Value]): (Array[Byte], String) = {
    val key = entry.getKey
    val value = entry.getValue

    val filename = key.getColumnQualifier.toString

    (value.get, filename)
  }

  private def putInternal(file: File, id: String, params: Map[String, String]): Unit = {
    val localName = getFileName(file, params.asJava)
    val bytes = ByteStreams.toByteArray(Files.asByteSource(file).openBufferedStream())

    val m = new Mutation(id)

    m.put(EMPTY_COLF, new Text(localName), new Value(bytes))
    bw.addMutation(m)
  }

  private def putInternal(fis: FileInputStream, id: String, fileName: String): Unit = {
    val m = new Mutation(id)
    val channel = fis.getChannel
    val mmb = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size())
    m.put(EMPTY_COLF, new Text(fileName), new Value(mmb))
    bw.addMutation(m)
    mmb.clear()
    channel.close()
    fis.close()
  }
}

object AccumuloBlobStore {
  val blobFeatureTypeName = "blob"

  val idFieldName = "storeId"
  val geomeFieldName = "geom"
  val filenameFieldName = "filename"
  val dateFieldName = "date"

  // TODO: Add metadata hashmap?
  val sftSpec = s"$filenameFieldName:String,$idFieldName:String,$geomeFieldName:Geometry,$dateFieldName:Date,thumbnail:String"

  val sft: SimpleFeatureType = SimpleFeatureTypes.createType(blobFeatureTypeName, sftSpec)
}

