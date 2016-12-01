package org.locationtech.geomesa.sparkgis.jdbc

import java.util

import org.geotools.data.DataAccessFactory
import org.geotools.jdbc.{JDBCDataStore, JDBCDataStoreFactory, SQLDialect}


class HiveJDBCDataStoreFactory extends JDBCDataStoreFactory {
  val DBTYPE: DataAccessFactory.Param = new DataAccessFactory.Param("dbtype", classOf[String], "Type", true, "hive")


  override def getDatabaseID: String = DBTYPE.sample.asInstanceOf[String]

  override def getValidationQuery: String = "select now()"

  override def getDriverClassName: String = "org.apache.hive.jdbc.HiveDriver"

  override def createSQLDialect(dataStore: JDBCDataStore): SQLDialect = new HiveDialect(dataStore)

  override def getDescription: String = "Spark HIVE JDBC Datastore"

  override def getJDBCUrl(params: util.Map[_, _]): String = {
    "jdbc:hive2://localhost:10000"
//    "jdbc:hive2://jupyter.ccri.com:10000"
  }

  //override def canProcess(params: util.Map[_, _]): Boolean = true //super.canProcess(params)

  override def setupParameters(parameters: util.Map[_, _]): Unit = super.setupParameters(parameters)
}
