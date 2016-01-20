/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.blob.api

import java.util.concurrent.ConcurrentHashMap

import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloDataStoreFactory}
import org.locationtech.geomesa.blob.core.AccumuloBlobStore
import org.locationtech.geomesa.web.core.GeoMesaBaseDataStoreServlet
import org.scalatra.{BadRequest, Ok}

import scala.collection.JavaConversions._
import scala.collection._

trait GeoMesaPersistentBlobStoreServlet extends GeoMesaBaseDataStoreServlet {

  val blobStores: concurrent.Map[String, AccumuloBlobStore] = new ConcurrentHashMap[String, AccumuloBlobStore]
  getPersistedDataStores.foreach {
    store => connectToBlobStore(store._2).map(abs => blobStores.putIfAbsent(store._1, abs))
  }

  // TODO: Revisit configuration and persistence of configuration.
  // https://geomesa.atlassian.net/browse/GEOMESA-958
  /**
    * Registers a data store, making it available for later use
    */
  post("/connections/:alias") {
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
  get("/connections/:alias") {
    try {
      getPersistedDataStore(params("alias"))
    } catch {
      case e: Exception => handleError(s"Error reading data store:", e)
    }
  }

  /**
    * Remove the reference to an existing data store
    */
  delete("/connections/:alias") {
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
  get("/connections/?") {
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

}