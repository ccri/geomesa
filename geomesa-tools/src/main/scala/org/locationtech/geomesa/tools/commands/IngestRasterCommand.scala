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
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.raster.ingest.LocalRasterIngest
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.commands.IngestRasterCommand.{Command, IngestRasterParameters}
import org.locationtech.geomesa.utils.ingest.Formats._

import scala.util.{Failure, Success}

class IngestRasterCommand(parent: JCommander) extends Command with AccumuloProperties {

  val params = new IngestRasterParameters()
  parent.addCommand(Command, params)

  override def execute() {
    val fmt = Option(params.format).getOrElse(getFileExtension(params.file))
    fmt match {
      case TIFF | DTED =>
        ingest
      case _           =>
        logger.error("Error: File format not supported for file " + params.file + ". Supported formats" +
          "are tif, tiff, dt0, dt1 and dt2")
    }
  }

  def ingest() {
    val ingester = new LocalRasterIngest(getRsaterIngestParams)
    ingester.runIngestTask() match {
      case Success(info) => logger.info("Ingestion is done.")
      case Failure(e) => throw new RuntimeException(e)
    }
  }

  def getRsaterIngestParams(): Map[String, Option[String]] = {
    Map(
      IngestRasterParams.ZOOKEEPERS        -> Some(Option(params.zookeepers).getOrElse(zookeepersProp)),
      IngestRasterParams.ACCUMULO_INSTANCE -> Some(Option(params.instance).getOrElse(instanceName)),
      IngestRasterParams.ACCUMULO_USER     -> Some(params.user),
      IngestRasterParams.ACCUMULO_PASSWORD -> Some(getPassword(params.password)),
      IngestRasterParams.AUTHORIZATIONS    -> Option(params.auths),
      IngestRasterParams.VISIBILITIES      -> Option(params.visibilities),
      IngestRasterParams.ACCUMULO_MOCK     -> Some(params.useMock.toString),
      IngestRasterParams.TABLE             -> Some(params.table),
      IngestRasterParams.FILE_PATH         -> Some(params.file),
      IngestRasterParams.FORMAT            -> Some(Option(params.format).getOrElse(getFileExtension(params.file))),
      IngestRasterParams.RASTER_NAME       -> Some(params.rasterName),
      IngestRasterParams.GEOSERVER_REG     -> Option(params.geoserverConf),
      IngestRasterParams.TIME              -> Option(params.timeStamp),
      IngestRasterParams.WRITE_MEMORY      -> Option(params.writeMemory),
      IngestRasterParams.WRITE_THREADS     -> Option(params.writeThreads).map(_.toString),
      IngestRasterParams.QUERY_THREADS     -> Option(params.queryThreads).map(_.toString),
      IngestRasterParams.SHARDS            -> Option(params.numShards).map(_.toString),
      IngestRasterParams.PARLEVEL          -> Some(params.parLevel.toString)
    )
  }
}

object IngestRasterCommand {
  val Command = "ingestRaster"

  @Parameters(commandDescription = "Ingest a raster file of various formats into GeoMesa")
  class IngestRasterParameters extends CreateRasterParams {
    @Parameter(names = Array("-fmt", "--format"), description = "Format of incoming raster data (tif | tiff | dt0 | " +
      "dt1 | dt2) to override file extension recognition")
    var format: String = null

    @Parameter(names = Array("-f", "--file"), description = "Raster file to be ingested", required = true)
    var file: String = null

    @Parameter(names = Array("-tm", "--timestamp"), description = "Raster file to be ingested")
    var timeStamp: String = null

    @Parameter(names = Array("-par", "--parallelLevel"), description = "Maximum number of threads for ingesting " +
      "multiple raster files")
    var parLevel: Int = 1
  }
}
