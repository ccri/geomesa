/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util

import org.geotools.data.DataAccessFactory
import org.geotools.jdbc._


class HiveJDBCDataStoreFactory extends JDBCDataStoreFactory {
  val DBTYPE: DataAccessFactory.Param = new DataAccessFactory.Param("dbtype", classOf[String], "Type", true, "hive")


  override def getDatabaseID: String = DBTYPE.sample.asInstanceOf[String]

  override def getValidationQuery: String = "select now()"

  override def getDriverClassName: String = "org.apache.hive.jdbc.HiveDriver"

  override def createSQLDialect(dataStore: JDBCDataStore): SQLDialect = new HiveDialect(dataStore)

  override def getDescription: String = "Spark HIVE JDBC Datastore"

  override def getJDBCUrl(params: util.Map[_, _]): String = {
    s"${params.get("namespace")}:${params.get("dbtype")}2://${params.get("host")}:${params.get("port")}"
  }

  override def setupParameters(parameters: util.Map[_, _]): Unit = super.setupParameters(parameters)
}
