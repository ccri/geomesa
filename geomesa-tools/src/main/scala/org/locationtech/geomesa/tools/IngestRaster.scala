/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.raster.ingest._
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

class IngestRaster() extends Logging with AccumuloProperties {

  //TODO: In the first round implementation, we just need subset of capabilities of
  //AccumuloDataStore: connection to accumulo table; writing mutations into table.
  //More efficient way of writing data onto table will be used, such as using Spark.
  def createDataStore(config: IngestArguments, password: String): AccumuloDataStore = {
    if (config.featureName.isEmpty) {
      logger.error("No feature name specified for raster feature ingest." +
        " Please check that all arguments are correct in the previous command. ")
      sys.exit()
    }

    val dsConfig = getAccumuloDataStoreConf(config, password)

    val ds = Option(DataStoreFinder.getDataStore(dsConfig).asInstanceOf[AccumuloDataStore])
    ds match {
      case None =>
        logger.error("Error, could not find data store with provided arguments." +
          " Please check that all arguments are correct in the previous command")
        sys.exit()
      case _ =>
    }
    ds.get
  }

  def getAccumuloDataStoreConf(config: IngestArguments, password: String) = Map(
    "instanceId" -> config.instanceName.getOrElse(instanceName),
    "zookeepers" -> config.zookeepers.getOrElse(zookeepers),
    "user" -> config.username,
    "password" -> password,
    "tableName" -> config.catalog,
    "auths" -> config.auths,
    "visibilities" -> config.visibilities,
    "maxShard" -> config.maxShards,
    "indexSchemaFormat" -> config.indexSchemaFmt
  ).collect {
    case (key, Some(value)) => (key, value);
    case (key, value: String) => (key, value)
  }

  def defineIngestJob(config: IngestArguments, password: String) = {
    IngestRaster.getFileExtension(config.file).toUpperCase match {
      case "TIFF" | "DTED" =>
        ingest(config, password)
      case _ =>
        logger.error(s"Error: file format not supported or not found in provided file path." +
          s" Supported formats include: TIF, TIFF and DT{0,1,2}. No data ingested.")

    }
  }

  //TODO: For now, a simple blocking method is used to ingest data from local FS.
  def ingest(config: IngestArguments, password: String): Unit = {
    val ds = createDataStore(config, password)

    val args: Map[String, Option[String]] = Map(
      IngestRasterParams.FILE_PATH -> Some(config.file),
      IngestRasterParams.FORMAT -> Some(IngestRaster.getFileExtension(config.file)),
      IngestRasterParams.FEATURE_NAME -> Some(config.featureName),
      IngestRasterParams.PREC -> config.prec.map(_.toString),
      IngestRasterParams.VISIBILITIES -> Some(ds.writeVisibilities),
      IngestRasterParams.ACCUMULO_INSTANCE -> Some(config.instanceName.getOrElse(instanceName)),
      IngestRasterParams.ZOOKEEPERS -> Some(config.zookeepers.getOrElse(zookeepers)),
      IngestRasterParams.ACCUMULO_USER -> Some(config.username),
      IngestRasterParams.ACCUMULO_PASSWORD -> Some(password),
      IngestRasterParams.CATALOG_TABLE -> Some(config.catalog),
      IngestRasterParams.AUTHORIZATIONS -> Some(ds.authorizationsProvider.getAuthorizations.toString)
    )

    val ingester = new SimpleRasterIngest(args, ds)
    ingester.runIngestTask() match {
      case Success(info) => logger.info("Ingestion is done.")
      case Failure(e) => throw new RuntimeException(e)
    }
  }
}

object IngestRaster extends App with Logging with GetPassword {
  val parser = new scopt.OptionParser[IngestArguments]("geomesa-tools ingestRaster") {
    implicit val optionStringRead: scopt.Read[Option[String]] = scopt.Read.reads(Option[String])
    head("GeoMesa Tools IngestRaster", "1.0")
    opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "Accumulo username" required()
    opt[Option[String]]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "Accumulo password, This can also be provided after entering a command" optional()
    opt[Option[String]]("instance-name").action { (s, c) =>
      c.copy(instanceName = s) } text "Accumulo instance name" optional()
    opt[Option[String]]('z', "zookeepers").action { (s, c) =>
      c.copy(zookeepers = s) } text "Accumulo Zookeepers string" optional()
    opt[String]('c', "catalog").action { (s, c) =>
      c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create" required()
    opt[Option[String]]('a', "auths") action { (s, c) =>
      c.copy(auths = s) } text "Accumulo auths (optional)" optional()
    opt[Option[String]]('v', "visibilities") action { (s, c) =>
      c.copy(visibilities = s) } text "Accumulo visibilities (optional)" optional()
    opt[Int]("shards") action { (i, c) =>
      c.copy(maxShards = Option(i)) } text "Accumulo number of shards to use (optional)" optional()
    opt[Int]("prec") action { (i, c) =>
      c.copy(maxShards = Option(i)) } text "Precision of ingested raster (optional)" optional()
    opt[String]('f', "feature-name").action { (s, c) =>
      c.copy(featureName = s) } text "the name of the feature" required()
    opt[String]("file").action { (s, c) =>
      c.copy(file = s) } text "the file to be ingested" required()
    help("help").text("show help command")
    checkConfig { c =>
      if (c.maxShards.isDefined && c.indexSchemaFmt.isDefined) {
        failure("Error: the options for setting the max shards and the indexSchemaFormat cannot both be set.")
      } else {
        success
      }
    }
  }

  try {
    parser.parse(args, IngestArguments()).map { config =>
      val pw = password(config.password)
      val ingestRaster = new IngestRaster()
      ingestRaster.defineIngestJob(config, pw)
    } getOrElse {
      logger.error("Error: command not recognized.")
    }
  }
  catch {
    case npe: NullPointerException => logger.error("Missing options and or unknown arguments on ingestRaster." +
                                                   "\n\t See 'geomesa ingestRaster --help'", npe)
  }

  def getFileExtension(file: String) = file.toLowerCase match {
    case geotiff if (file.endsWith("tif") || file.endsWith("tiff"))                     => "TIFF"
    case dted if (file.endsWith("dt0") || file.endsWith("dt1") || file.endsWith("dt2")) => "DTED"
    case _                                                                              => "NOTSUPPORTED"
  }
}
