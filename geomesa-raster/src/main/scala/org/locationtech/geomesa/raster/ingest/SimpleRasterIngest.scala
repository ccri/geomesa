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

import com.twitter.scalding.{Args, Job}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom._
import org.geotools.factory.Hints
import org.joda.time.DateTime
import org.locationtech.geomesa.core.data.AccumuloDataStore
import com.ccri.commons.util.time.TimeUtils
import scala.util.Try
import org.locationtech.geomesa.utils.geohash.{TwoGeoHashBoundingBox, BoundingBoxGeoHashIterator, GeoHash}
import java.io.File
import javax.media.jai.{JAI, ImageLayout}
import java.awt.RenderingHints
import org.geotools.coverageio.gdal.dted.DTEDReader
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.{Transaction, DataStoreFinder}
import com.ccri.commons.geo.interpolation.Interpolator
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader
import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import java.util.UUID
import org.apache.accumulo.core.data.{Value, Mutation}
import org.apache.hadoop.io.Text
import org.apache.accumulo.core.security.ColumnVisibility
import java.nio.ByteBuffer
import java.nio.charset.Charset
import org.apache.accumulo.core.client.{Connector, BatchWriterConfig}

class SimpleRasterIngest(config: Map[String, Option[String]], ds: AccumuloDataStore) extends Logging {
  import scala.collection.JavaConversions._

  lazy val path             = config(IngestRasterParams.FILE_PATH).get
  lazy val format           = config(IngestRasterParams.FORMAT).get
  lazy val featureName      = config(IngestRasterParams.FEATURE_NAME).get
  lazy val precO            = config(IngestRasterParams.PREC).map(_.toInt)
  lazy val visibilitiesO    = config(IngestRasterParams.VISIBILITIES)

  val METADATA_ROW  = new Text("~METADATA")

  val bwConfig =
    new BatchWriterConfig().setMaxMemory(10000L).setMaxWriteThreads(1)

  def runIngestTask() = Try {
    val file = new File(path)
    val rasterMetadata = rasterMetadataFromFile(file, format)
    val prec = precO.getOrElse(rasterMetadata.prec)
    val visibilities = visibilitiesO.get
    val surfaceId = getSurfaceId(prec)
    val interpCoverage =generateInterpolator(rasterMetadata, new File(path), format, prec)
    val ll = GeoHash.apply(rasterMetadata.minX, rasterMetadata.minY, prec)
    val ur = GeoHash.apply(rasterMetadata.maxX, rasterMetadata.maxY, prec)
    val ghIter = new BoundingBoxGeoHashIterator(new TwoGeoHashBoundingBox(ll, ur))
    val timestamp = TimeUtils.dateToAccTimestamp(TimeUtils.utcDateTime)

    val mutations: Seq[Mutation] = ghIter.map(gh => {
      val rasterValue: Option[Double] = interpCoverage.getValue(gh.getPoint)
      rasterValue match {
        case Some(raster) =>
          createMutation(gh, surfaceId, visibilities, timestamp, raster)
        case _ =>
          logger.warn(s"Failed to evaluate interpCoverage at ${gh.hash}")
          //TODO: Test. Find better way to define target coverage
          //Target grid construsted from ll and ur is a bit too big. Interpolation
          //fails at some edge points, set value to 0.0D for those points for now.
          createMutation(gh, surfaceId, visibilities, timestamp, 0.0D)
      }
    }).toList

    //TODO: It uses naive way of writing data to accumulo table. Will be replaced with
    //more efficient way.
    writeMutations(ds.connector, ds.catalogTable, mutations: _*)

    //TODO: Skip ingesting metadata for now. Geoserver CoverageReader doesn't really
    //need metadata to construct output image.
//    val basicMetaProperties: Map[String, String] = Map (
//      "type" -> "modelThreat",
//      "model.id" -> "4efd2680-a613-4c6f-b06f-8b7febf2a7f1",
//      "dow" -> "false",
//      "tod" -> "false",
//      "threat.type" -> "overallranked",
//      "timestamp" -> "1414515456"
//    )
//
//    val optionalMetaProperties: Map[String, String] = Map (
//      "count" -> "0",
//      "maximum" -> "0",
//      "minimum" -> "0",
//      "mean" -> "0",
//      "variance" -> "0",
//      "version" -> "0"
//    )

    GeoserverClientService.registerSurface(surfaceId, prec, None, config)
  }

  def getRow(point: GeoHash) = new Text(point.hash)

  def createMutation(gh: GeoHash, surfaceId: String, visibilities: String, timestamp: Long, rasterVal: Double):
    Mutation ={
    val mutation = new Mutation(getRow(gh))
    val colFam = new Text(surfaceId)
    val colQual = new Text("")
    val colVis = new ColumnVisibility(visibilities)
    val value = new Value(Utils.doubleToBytes(rasterVal))
    mutation.put(colFam, colQual, colVis, timestamp, value)
    mutation
  }

  /**
   * Handles writing mutations
   *
   * @param mutations
   */
  def writeMutations(connector: Connector, table: String, mutations: Mutation*): Unit = {
    val writer = ds.connector.createBatchWriter(table, bwConfig)
    for (mutation <- mutations) {
      writer.addMutation(mutation)
    }
    writer.flush()
    writer.close()
  }

//    def saveSurfaceMetadata(properties: Map[String, String]) {
//    val mutation = new Mutation(METADATA_ROW)
//    properties.foreach { case (propertyName, propertyValue) =>
//      mutation.put(new Text(surface.id),
//        new Text(propertyName),
//        defaultCV,
//        new Value(Utils.stringToBytes(propertyValue)))
//    }
//  }

  //TODO: find better way to define surface id.
  def getSurfaceId(prec: Int): String =
    s"${featureName}_${prec}_${UUID.randomUUID.toString.substring(0, 4)}"

  def rasterMetadataFromFile(imageFile: File, imageType: String): RasterSurfaceMetadata = {
    val reader = getReader(imageFile, imageType)

    //    val reader = new GeoTiffReader(imageFile, new Hints(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true))
    val gcOrig: GridCoverage2D = reader.read(null)
    val envelope = gcOrig.getEnvelope
    val lonSpan = envelope.getSpan(0)
    val latSpan = envelope.getSpan(1)
    val llx = envelope.getMinimum(0)
    val lly = envelope.getMinimum(1)
    val gridRange = gcOrig.getGridGeometry.getGridRange
    val width = gridRange.getSpan(0)
    val height = gridRange.getSpan(1)
    val longBlockSpan = lonSpan / width
    val latBlockSpan = latSpan / height
    val numLongitudeDivisions = math.log(360.0 / longBlockSpan) / math.log(2.0)
    val numLatitudeDivisions = math.log(180.0 / latBlockSpan) / math.log(2.0)

    // precision is the sum of these two and floor to make sure
    //that the RasterSurfaceMetadata contains fucntion thinks
    // it is at least as large as the original surface
    val prec = math.floor(numLatitudeDivisions + numLongitudeDivisions).toInt

    RasterSurfaceMetadata(Double.MinValue,
                          Double.MaxValue,
                          GeoHash(llx, lly, prec),
                          prec,
                          width,
                          height,
                          new DateTime(0),
                          "dt0")
  }

  def generateInterpolator(rasterMetadata: RasterSurfaceMetadata,
                           imageFile: File,
                           imageType: String,
                           targetPrec: Int): Interpolator = {
    val reader = getReader(imageFile, imageType)
    val gcOrig: GridCoverage2D = reader.read(null)
    Interpolator(gcOrig, aligned = true, rasterMetadata.prec / targetPrec)
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

case class RasterSurfaceMetadata(minVal: Double,
                                 maxVal: Double,
                                 ll: GeoHash,
                                 prec: Int,
                                 width: Int,
                                 height: Int,
                                 time: DateTime,
                                 fileType: String) {
  private lazy val gf = new GeometryFactory(new PrecisionModel, 4326)
  val minX = ll.x - (ll.bbox.longitudeSize / 2)
  val minY = ll.y - (ll.bbox.latitudeSize / 2)
  val maxX = minX + ll.bbox.longitudeSize * width
  val maxY = minY + ll.bbox.latitudeSize * height
  private lazy val rasterEnvelope = {
    gf.toGeometry(new Envelope(minX, maxX, minY, maxY))
  }

  private lazy val bufferedGeometry = rasterEnvelope.buffer(GeoHash.longitudeDeltaForPrecision(prec))

  def contains(geom: Geometry) =
    bufferedGeometry contains geom

  def disjoint(geom: Geometry) =
    bufferedGeometry disjoint geom

  def printInfo() {
    println("RasterSurfaceMetadata:")
    println("minVal: " + minVal + ", maxVal: " + maxVal + ", prec: " + prec + ", width: " + width + ", " +
      "height: " + height)
    println("Envelope: minX: " + minX + ", minY: " + minY +
      "maxX: " + maxX + ", maxY: " + maxY)
  }
}

object IngestRasterParams {
  val ACCUMULO_INSTANCE   = "geomesa-tools.ingest.instance"
  val ZOOKEEPERS          = "geomesa-tools.ingest.zookeepers"
  val ACCUMULO_MOCK       = "geomesa-tools.ingest.useMock"
  val ACCUMULO_USER       = "geomesa-tools.ingest.user"
  val ACCUMULO_PASSWORD   = "geomesa-tools.ingest.password"
  val AUTHORIZATIONS      = "geomesa-tools.ingest.authorizations"
  val VISIBILITIES        = "geomesa-tools.ingest.visibilities"
  val SHARDS              = "geomesa-tools.ingest.shards"
  val PREC                = "geomesa-tools.ingest.prec"
  val FILE_PATH           = "geomesa-tools.ingest.path"
  val FORMAT              = "geomesa-tools.ingest.delimiter"
  val FEATURE_NAME        = "geomesa-tools.feature.name"
  val CATALOG_TABLE       = "geomesa-tools.feature.tables.catalog"

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

object Utils {
  val doubleSize = 8
  implicit def doubleToBytes(d: Double): Array[Byte] = {
    val bytes = new Array[Byte](doubleSize)
    ByteBuffer.wrap(bytes).putDouble(d)
    bytes
  }
  implicit def bytesToDouble(bs: Array[Byte]): Double = ByteBuffer.wrap(bs).getDouble

  val intSize = 4
  implicit def intToBytes(i: Int): Array[Byte] = {
    val bytes = new Array[Byte](intSize)
    ByteBuffer.wrap(bytes).putInt(i)
    bytes
  }
  implicit def bytesToInt(bs: Array[Byte]): Int = ByteBuffer.wrap(bs).getInt

  val longSize = 8
  implicit def longToBytes(l: Long): Array[Byte] = {
    val bytes = new Array[Byte](longSize)
    ByteBuffer.wrap(bytes).putLong(l)
    bytes
  }
  implicit def bytesToLong(bs: Array[Byte]): Long = ByteBuffer.wrap(bs).getLong

  val utf8Charset = Charset.forName("UTF-8")
  implicit def stringToBytes(s: String): Array[Byte] = s.getBytes(utf8Charset)
  implicit def bytesToString(bs: Array[Byte]): String = new String(bs, utf8Charset)
}
