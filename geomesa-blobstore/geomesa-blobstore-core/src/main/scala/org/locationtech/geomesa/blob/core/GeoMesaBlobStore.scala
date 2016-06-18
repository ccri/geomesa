package org.locationtech.geomesa.blob.core

import java.io.File
import java.util
import java.util.Map.Entry

import com.google.common.io.Files
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureStore
import org.geotools.data.{DataStore, Query}
import org.geotools.filter.identity.FeatureIdImpl
import org.opengis.filter.Filter
import org.locationtech.geomesa.blob.core.GeoMesaBlobStoreSFT._
import org.locationtech.geomesa.blob.core.handlers.{BlobStoreByteArrayHandler, BlobStoreFileHandler}
import org.locationtech.geomesa.utils.filters.Filters
import org.locationtech.geomesa.utils.geotools.Conversions.{RichSimpleFeature, _}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

abstract class GeoMesaBlobStore(ds: DataStore, bs: BlobStore) extends GeoBlobStore {

  protected val fs = ds.getFeatureSource(BlobFeatureTypeName).asInstanceOf[SimpleFeatureStore]

  override def get(id: String): Entry[String, Array[Byte]] = {
    bs.get(id)
  }

  override def put(file: File, params: util.Map[String, String]): String = {
    BlobStoreFileHandler.buildSF(file, params.toMap).map { sf =>
      val bytes = Files.toByteArray(file)
      putInternalSF(sf, bytes)
    }.orNull
  }

  override def put(bytes: Array[Byte], params: util.Map[String, String]): String = {
    val sf = BlobStoreByteArrayHandler.buildSF(params)
    putInternalSF(sf, bytes)
  }

  private def putInternalSF(sf: SimpleFeature, bytes: Array[Byte]): String = {
    val id = sf.get[String](IdFieldName)
    val localName = sf.get[String](FilenameFieldName)
    fs.addFeatures(new ListFeatureCollection(sft, List(sf)))
    bs.put(id, localName, bytes)
    id
  }

  override def delete(id: String): Unit = {
    val removalFilter = Filters.ff.id(new FeatureIdImpl(id))
    fs.removeFeatures(removalFilter)
    bs.deleteBlob(id)
  }

  override def deleteBlobStore(): Unit = {
    ds.removeSchema(BlobFeatureTypeName)
    bs.deleteBlobStore()
  }

  override def getIds(filter: Filter): util.Iterator[String] = {
    getIds(new Query(BlobFeatureTypeName, filter))
  }

  override def getIds(query: Query): util.Iterator[String] = {
    fs.getFeatures(query).features.map(_.get[String](IdFieldName))
  }

  override def close(): Unit = {
    bs.close()
  }
}
