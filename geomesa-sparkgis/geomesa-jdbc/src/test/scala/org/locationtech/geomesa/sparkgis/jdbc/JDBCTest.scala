package org.locationtech.geomesa.sparkgis.jdbc

import org.geotools.data.DataStoreFinder
import scala.collection.JavaConversions._

object JDBCTest extends App {

  val dsParams: java.util.Map[String, String] = Map[String, String]("dbtype" -> "hive", "host" -> "localhost", "port" -> "10000", "user" -> "jupyter", "password" -> "")

  DataStoreFinder.getAllDataStores.foreach{ d => println(s"Data: $d") }

  val canProcess = (new HiveJDBCDataStoreFactory).canProcess(dsParams)

  println(s"Can process: $canProcess")

  val ds = DataStoreFinder.getDataStore(dsParams)

  println("DS has tables/types:")
  ds.getNames.foreach{println}


}
