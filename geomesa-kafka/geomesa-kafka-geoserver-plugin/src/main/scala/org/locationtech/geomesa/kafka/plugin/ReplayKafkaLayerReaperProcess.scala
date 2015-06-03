/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.kafka.plugin

import com.typesafe.scalalogging.slf4j.Logging
import org.geoserver.catalog.event._
import org.geoserver.catalog.{Catalog, FeatureTypeInfo, LayerInfo}
import org.geotools.data.DataStore
import org.geotools.process.factory.{DescribeProcess, DescribeResult}
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.kafka.plugin.ReplayKafkaDataStoreProcess._

import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.control.NonFatal

@DescribeProcess(
  title = "GeoMesa Replay Kafka Layer Reaper",
  description = "Removes Kafka Replay Layers from GeoServer",
  version = "1.0.0"
)
class ReplayKafkaLayerReaperProcess(val catalog: Catalog, val hours: Int)
  extends GeomesaKafkaProcess with Logging with Runnable {

  import ReplayKafkaLayerReaper._

  // register a listener to handle user deletion of replay layers
  catalog.addListener(new ReplayKafkaLayerCatalogListener())

  @DescribeResult(name = "result",
                  description = "If all eligible layers were removed successfully, true, otherwise false.")
  def execute(): Boolean = {
    Try {
      val currentTime: Instant = long2Long(System.currentTimeMillis())
      val ageLimit: Instant = currentTime.minus(Duration.standardHours(hours))

      var error = false

      for {
        layer <- catalog.getLayers
        age <- getVolatileAge(layer)
        if age.isBefore(ageLimit)
        sftName <- getSftName(layer)
        ds <- getDataStore(layer)
      } {
        try {
          logger.debug(s"Cleaning up replay layer $layer for simple feature type $sftName")
          catalog.remove(layer)
          ds.removeSchema(sftName)
        } catch {
          case NonFatal(e) =>
            logger.error(s"Error cleaning up replay layer $layer for simple feature type $sftName", e)
            error = true
        }
      }

      error
    }.getOrElse(false)
  }

  private val message = "Running Replay Kafka Layer Cleaner"

  override def run(): Unit = {
    logger.info(message)
    execute()
  }
}

class ReplayKafkaLayerCatalogListener extends CatalogListener with Logging {

  import ReplayKafkaLayerReaper._

  override def handleRemoveEvent(event: CatalogRemoveEvent): Unit = event.getSource match {
    case layer: LayerInfo =>
      Try {
        for {
          sftName <- getSftName(layer)
          ds <- getDataStore(layer)
        } {
          try {
            logger.debug(s"Cleaning up replay layer $layer for simple feature type $sftName")
            ds.removeSchema(sftName)
          } catch {
            case NonFatal(e) =>
              logger.error(s"Error cleaning up replay layer $layer for simple feature type $sftName", e)
          }
        }
      }
    case _ =>
  }

  override def reloaded(): Unit = {}

  override def handleAddEvent(event: CatalogAddEvent): Unit = {}

  override def handlePostModifyEvent(event: CatalogPostModifyEvent): Unit = {}

  override def handleModifyEvent(event: CatalogModifyEvent): Unit = {}
}

object ReplayKafkaLayerReaper extends Logging {

  def getDataStore(layerInfo: LayerInfo): Option[DataStore] = layerInfo.getResource match {
    case fti: FeatureTypeInfo => Some(fti.getStore.getDataStore(null).asInstanceOf[DataStore])
    case _ => None
  }
}