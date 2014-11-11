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
package org.locationtech.geomesa.raster.ingest

import org.geotools.referencing.crs.DefaultGeographicCRS
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.factory.Hints
import org.geotools.geometry.Envelope2D
import org.joda.time.{DateTimeZone, DateTime}
import org.joda.time.format.DateTimeFormat
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.utils.geohash.GeoHash
import java.io.File
import java.util.UUID
import javax.media.jai.{JAI, ImageLayout}
import java.awt.RenderingHints
import org.geotools.coverageio.gdal.dted.DTEDReader
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.accumulo.core.client.BatchWriterConfig
import com.vividsolutions.jts.geom.{Coordinate, Point}
import org.opengis.referencing.crs.CoordinateReferenceSystem
import scala.util.Try

class SimpleRasterIngest(config: Map[String, Option[String]], cs: AccumuloCoverageStore) extends Logging {

  lazy val path             = config(IngestRasterParams.FILE_PATH).get
  lazy val fileType         = config(IngestRasterParams.FILE_TYPE).get
  lazy val rasterName       = config(IngestRasterParams.RASTER_NAME).get
  lazy val visibilities     = config(IngestRasterParams.VISIBILITIES).get

  val bwConfig =
    new BatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(1)

  val df = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def runIngestTask() = Try {
    val file = new File(path)
    val ingestTime = config(IngestRasterParams.TIME).map(df.parseDateTime(_)).getOrElse(new DateTime(DateTimeZone.UTC))
    val rasterMetadata = rasterMetadataFromFile(file, fileType, ingestTime)

    val rasterReader = getReader(file, fileType)
    val rasterGrid: GridCoverage2D = rasterReader.read(null)

    cs.saveRaster(rasterGrid, rasterMetadata)

    GeoserverClientService.registerSurface(rasterName, None, config)
    println("Ingestion is done!")
  }

  /**
   * Find GeoHash instance with maximum precision that covers envelope defined by two points.
   *
   * @param ll Low left point of bounding box
   * @param ur Up right point of bounding box
   * @return GeoHash instance
   */
  def getMBGH(ll: Point, ur: Point): GeoHash = {
    val width = ur.getX - ll.getX
    val height = ur.getY - ll.getY
    require(width >= 0 && height >= 0, s"Wrong width $width and height $height of input bounding box, cannot process")

    (GeoHash.MAX_PRECISION to 0 by -1).foreach(prec => {
      val lonDelta = GeoHash.longitudeDeltaForPrecision(prec)
      val latDelta = GeoHash.latitudeDeltaForPrecision(prec)
      if (lonDelta >= width && latDelta >= height) {
        val geo = GeoHash(ll.getX, ll.getY, prec)
        if (geo.bbox.covers(ur)) return geo
      }
    })
    null
  }

  def getMBGH(minX: Double, minY: Double, maxX: Double, maxY: Double): GeoHash =
    getMBGH(GeoHash.factory.createPoint(new Coordinate(minX, minY)),
            GeoHash.factory.createPoint(new Coordinate(maxX, maxY)))

  def getRasterId(rasterName: String): String =
    s"${rasterName}_${UUID.randomUUID.toString}"

  def rasterMetadataFromFile(imageFile: File, imageType: String, time: DateTime): RasterMetadata = {
    val reader = getReader(imageFile, imageType)
    val gcOrig: GridCoverage2D = reader.read(null)
    val crs = gcOrig.getCoordinateReferenceSystem2D
    val envelope = gcOrig.getEnvelope2D
    val mbgh = getMBGH(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val id = getRasterId(rasterName)
    RasterMetadata(id, envelope, mbgh, time, imageType, crs)
  }

  def getReader(imageFile: File, imageType: String): AbstractGridCoverage2DReader = {
    imageType match {
      case "TIFF" => getTiffReader(imageFile)
      case "DTED" => getDtedReader(imageFile)
      case _ => throw new Exception("Image type is not supported.")
    }
  }

  def getTiffReader(imageFile: File): AbstractGridCoverage2DReader = {
    new GeoTiffReader(imageFile, new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true))
  }

  def getDtedReader(imageFile: File): AbstractGridCoverage2DReader = {
    val l = new ImageLayout()
    l.setTileGridXOffset(0).setTileGridYOffset(0).setTileHeight(512).setTileWidth(512)
    val hints = new Hints
    hints.add(new RenderingHints(JAI.KEY_IMAGE_LAYOUT, l))
    new DTEDReader(imageFile, hints)
  }
}

case class RasterMetadata(id: String,
                          envelope: Envelope2D,
                          mbgh: GeoHash, //Minimum bounding box GeoHash
                          time: DateTime,
                          fileType: String,
                          crs: CoordinateReferenceSystem = DefaultGeographicCRS.WGS84,
                          band: Int = 0)

object IngestRasterParams {
  val ACCUMULO_INSTANCE   = "geomesa-tools.ingest.instance"
  val ZOOKEEPERS          = "geomesa-tools.ingest.zookeepers"
  val ACCUMULO_MOCK       = "geomesa-tools.ingest.useMock"
  val ACCUMULO_USER       = "geomesa-tools.ingest.user"
  val ACCUMULO_PASSWORD   = "geomesa-tools.ingest.password"
  val AUTHORIZATIONS      = "geomesa-tools.ingest.authorizations"
  val VISIBILITIES        = "geomesa-tools.ingest.visibilities"
  val FILE_PATH           = "geomesa-tools.ingest.path"
  val FILE_TYPE           = "geomesa-tools.ingest.filetype"
  val TIME                = "geomesa-tools.ingest.time"
  val RASTER_NAME         = "geomesa-tools.raster.name"
  val TABLE               = "geomesa-tools.raster.table"
}

object ActorSystemGenerator {
  def getActorSystem(name: String, configStr: String): ActorSystem = {
    val config = ConfigFactory.parseString(conf())
    ActorSystem(name, config)
  }

  def conf() = s"""
    akka {
      actor {
        default-dispatcher {
          executor = fork-join-executor
          fork-join-executor {
            parallelism-min = 8
            parallelism-factor = 3
            parallelism-max = 64
          }
          thread-pool-executor {
            core-pool-size-min = 8
            core-pool-size-factor = 3
            core-pool-size-max = 64
            max-pool-size-min = 8
            max-pool-size-factor = 3
            max-pool-size-max = 64
          }
        }
      }
    }"""
}
