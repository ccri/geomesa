/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.api

import java.io.{File, IOException}
import java.nio.file.attribute.PosixFilePermission._
import java.nio.file.attribute.PosixFilePermissions
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import javax.servlet.http.HttpServletRequest

import org.apache.commons.io.FilenameUtils
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore
import org.locationtech.geomesa.blob.core.AccumuloBlobStore._
import org.locationtech.geomesa.utils.cache.FilePersistence
import org.locationtech.geomesa.web.core.GeoMesaBaseDataStoreServlet
import org.scalatra._
import org.scalatra.servlet.{FileUploadSupport, MultipartConfig, SizeConstraintExceededException}

import scala.collection.JavaConversions._
import scala.collection.{Map, concurrent}

class BlobstoreServlet(val persistence: FilePersistence) extends GeoMesaBaseDataStoreServlet with FileUploadSupport with GZipSupport {
  override def root: String = "bs"

  val maxFileSize: Int = System.getProperty(BlobstoreServlet.maxFileSizeSysProp, "50").toInt
  val maxRequestSize: Int = System.getProperty(BlobstoreServlet.maxRequestSizeSysProp, "100").toInt

  // caps blob size
  configureMultipartHandling(
    MultipartConfig(
      maxFileSize = Some(maxFileSize * 1024 * 1024),
      maxRequestSize = Some(maxRequestSize * 1024 * 1024)
    )
  )
  error {
    case e: SizeConstraintExceededException =>
      handleError("Uploaded file too large!", e)
    case e: IOException =>
      handleError("IO exception in BlobstoreServlet", e)
  }

  val blobStores: concurrent.Map[String, AccumuloBlobStore] = new ConcurrentHashMap[String, AccumuloBlobStore]
  getPersistedDataStores.foreach {
    store => connectToBlobStore(store._2).map(abs => blobStores.putIfAbsent(store._1, abs))
  }

  override def requestPath(implicit request: HttpServletRequest): String = {
    if (request.getMethod == "POST" && request.getServletPath.startsWith("/blob")){
      request.getServletPath + request.getPathInfo
    } else {
      super.requestPath
    }
  }

  // TODO: Revisit configuration and persistence of configuration.
  // https://geomesa.atlassian.net/browse/GEOMESA-958
  /**
    * Registers a data store, making it available for later use
    */
  post("/ds/:alias") {
    logger.debug("Attempting to register accumulo connection in Blob Store")
    val dsParams = datastoreParams
    val ds = new AccumuloDataStoreFactory().createDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    if (ds == null) {
      BadRequest(reason = "Could not load data store using the provided parameters.")
    } else {
      val alias = params("alias")
      val prefix = keyFor(alias)
      val toPersist = dsParams.map { case (k, v) => keyFor(alias, k) -> v }
      try {
        persistence.removeAll(persistence.keys(prefix).toSeq)
        persistence.persistAll(toPersist)
        blobStores.put(alias, new AccumuloBlobStore(ds))
        Ok()
      } catch {
        case e: Exception => handleError(s"Error persisting data store '$alias':", e)
      }
    }
  }

  /**
    * Retrieve an existing data store
    */
  get("/ds/:alias") {
    try {
      getPersistedDataStore(params("alias"))
    } catch {
      case e: Exception => handleError(s"Error reading data store:", e)
    }
  }

  /**
    * Remove the reference to an existing data store
    */
  delete("/ds/:alias") {
    val alias = params("alias")
    val prefix = keyFor(alias)
    try {
      persistence.removeAll(persistence.keys(prefix).toSeq)
      Ok()
    } catch {
      case e: Exception => handleError(s"Error removing data store '$alias':", e)
    }
  }

  /**
    * Retrieve all existing data stores
    */
  get("/ds/?") {
    try {
      getPersistedDataStores
    } catch {
      case e: Exception => handleError(s"Error reading data stores:", e)
    }
  }

  private def connectToBlobStore(dsParams: Map[String, String]): Option[AccumuloBlobStore] = {
    val ds = new AccumuloDataStoreFactory().createDataStore(dsParams).asInstanceOf[AccumuloDataStore]
    if (ds == null) {
      logger.error("Bad Connection Params: {}", dsParams)
      None
    } else {
      Some(new AccumuloBlobStore(ds))
    }
  }

  delete("/blob/:alias/:id") {
    val alias = params("alias")
    blobStores.get(alias) match {
      case None =>
        BadRequest(reason = "AccumuloBlobStore is not initialized.")
      case Some(abs) =>
        val id = params("id")
        logger.debug("Attempting to delete: {} from store: {}", id, alias)
        abs.delete(id)
        Ok(reason = s"deleted feature: $id")
    }
  }

  get("/blob/:alias/:id") {
    val alias = params("alias")
    blobStores.get(alias) match {
      case None =>
        BadRequest(reason = "AccumuloBlobStore is not initialized.")
      case Some(abs) =>
        val id = params("id")
        logger.debug("Attempting to get blob for id: {} from store: {}", id, alias)
        val (returnBytes, filename) = abs.get(id)
        if (returnBytes == null) {
          NotFound(reason = s"Unknown ID $id")
        } else {
          contentType = "application/octet-stream"
          response.setHeader("Content-Disposition", "attachment;filename=" + filename)
          Ok(returnBytes)
        }
    }
  }

  // scalatra routes bottom up, so we want the ds post to be checked first
  post("/blob/:alias") {
    val alias = params("alias")
    blobStores.get(alias) match {
      case None      =>
        logger.error("AccumuloBlobStore is not initialized in BlobStore.")
        BadRequest()
      case Some(abs) =>
        logger.debug("Attempting to ingest file to BlobStore")
        try {
          fileParams.get("file") match {
            case None =>
              logger.error("no file parameter in request")
              BadRequest()
            case Some(file) =>
              val otherParams = collection.mutable.Map[String, String]()
              multiParams.foreach{ case (s, p) => otherParams.update(s, p.head) }
              if (!otherParams.contains(filenameFieldName)) {
                // we put the true filename in here so that we can preserve it in the blob table
                otherParams.put(filenameFieldName, file.getName)
              }
              val tempFile = File.createTempFile(UUID.randomUUID().toString, FilenameUtils.getExtension(file.getName))
              val actRes = try {
                file.write(tempFile)
                abs.put(tempFile, otherParams.toMap) match {
                  case Some(id) =>
                    Created(body = id, headers = scala.collection.immutable.Map("Location" -> request.getRequestURL.append(id).toString))
                  case None =>
                    BadRequest(reason = "Unable to process file")
                }
              } catch {
                case e: Exception => handleError("", e)
              } finally {
                tempFile.delete()
              }
              actRes
          }
        } catch {
          case e: Exception => handleError("Error uploading file", e)
        }
    }
  }

}

object BlobstoreServlet {
  val permissions  = PosixFilePermissions.asFileAttribute(Set(OWNER_READ, OWNER_WRITE, OWNER_EXECUTE, GROUP_READ, GROUP_WRITE))
  val maxFileSizeSysProp = "org.locationtech.geomesa.blob.api.maxFileSizeMB"
  val maxRequestSizeSysProp = "org.locationtech.geomesa.blob.api.maxRequestSizeMB"
}
