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

import java.lang.{Long => JLong}

import com.typesafe.scalalogging.slf4j.Logging
import org.geoserver.catalog._
import org.geotools.data.DataStore
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.kafka.{KafkaDataStoreHelper, ReplayConfig}
import org.opengis.feature.simple.SimpleFeatureType

import scala.language.implicitConversions

@DescribeProcess(
  title = "GeoMesa Build Replay From KafkaDataStore",
  description = "Builds a replay layer from a defined window of time on a KafkaDataStore",
  version = "1.0.0"
)
class ReplayKafkaDataStoreProcess(val catalog: Catalog) extends GeomesaKafkaProcess with Logging {
  import org.locationtech.geomesa.kafka.plugin.ReplayKafkaDataStoreProcess._
  @DescribeResult(name = "result", description = "Name of the Layer created for the Kafka Window")
  def execute(
              @DescribeParameter(name = "features", description = "Source GeoServer Feature Collection, used for SFT.")
              features: SimpleFeatureCollection,
              @DescribeParameter(name = "workspace", description = "Workspace of Store.")
              workspace: String,
              @DescribeParameter(name = "store", description = "Store of features")
              store: String,
              @DescribeParameter(name = "startTime", description = "POSIX Start Time of the replay window (in milliseconds).")
              startTime: java.lang.Long,
              @DescribeParameter(name = "endTime", description = "POSIX End Time of the replay window (in milliseconds).")
              endTime: java.lang.Long,
              @DescribeParameter(name = "readBehind", description = "The amount of time to pre-read in milliseconds.")
              readBehind: java.lang.Long
              ): String = {

    val storeInfo: DataStoreInfo = getDataStoreInfo(workspace, store)
    val workspaceInfo: WorkspaceInfo = storeInfo.getWorkspace

    // set the catalogBuilder to our store
    val catalogBuilder = new CatalogBuilder(catalog)
    catalogBuilder.setWorkspace(workspaceInfo)
    catalogBuilder.setStore(storeInfo)

    // parse time parameters and make Replay Config
    val replayConfig = new ReplayConfig(startTime, endTime, readBehind)

    // create volatile SFT
    val volatileSFT = createVolatileSFT(storeInfo, features, replayConfig)

    // build the type info
    val volatileTypeInfo = catalogBuilder.buildFeatureType(volatileSFT.getName)
    catalogBuilder.setupBounds(volatileTypeInfo)

    // build the layer and mark as volatile
    val volatileLayerInfo = catalogBuilder.buildLayer(volatileTypeInfo)
    injectMetadata(volatileLayerInfo, volatileSFT)

    // add new layer with hints to GeoServer
    catalog.add(volatileTypeInfo)
    catalog.add(volatileLayerInfo)

    s"created layer: ${volatileLayerInfo.getName}"
  }

  private def createVolatileSFT(storeInfo: DataStoreInfo,
                                features: SimpleFeatureCollection,
                                rConfig: ReplayConfig): SimpleFeatureType = {

    val ds = storeInfo.getDataStore(null).asInstanceOf[DataStore]

    // go back to the DS to get the schema to ensure that it contains all required user data
    val extantSFT: SimpleFeatureType = ds.getSchema(features.getSchema.getTypeName)

    val replaySFT: SimpleFeatureType = KafkaDataStoreHelper.createReplaySFT(extantSFT, rConfig)

    // check for existing layer
    if (checkForLayer(storeInfo.getWorkspace.getName, replaySFT.getTypeName)) {
      throw new ProcessException(s"Target layer already exists for SFT: ${replaySFT.getTypeName}")
    }

    ds.createSchema(replaySFT)

    //verify by retrieving the stored sft
    ds.getSchema(replaySFT.getName)
  }

  private def getDataStoreInfo(workspace: String, dataStore: String): DataStoreInfo = {
    val wsInfo = Option(catalog.getWorkspaceByName(workspace)).getOrElse(catalog.getDefaultWorkspace)

    Option(catalog.getDataStoreByName(wsInfo.getName, dataStore)).getOrElse {
      throw new ProcessException(s"Unable to find store $dataStore in source workspace ${wsInfo.getName}")
    }
  }

  private def checkForLayer(workspace: String, sftTypeName: String): Boolean = {
    val layerName = s"$workspace:$sftTypeName"
    catalog.getLayerByName(layerName) != null
  }

}

object ReplayKafkaDataStoreProcess {
  val volatileLayerSftHint: String = "kafka.geomesa.volatile.layer.sft"
  val volatileLayerAgeHint: String = "kafka.geomesa.volatile.layer.age"

  implicit def longToInstant(l: JLong): Instant = new Instant(l)
  implicit def longToDuration(l: JLong): Duration = new Duration(l)

  def injectMetadata(info: LayerInfo, sft: SimpleFeatureType): Unit = {
    info.getMetadata.put(volatileLayerSftHint, sft.getTypeName)
    info.getMetadata.put(volatileLayerAgeHint, System.currentTimeMillis().asInstanceOf[JLong])
  }

  def getSftName(info: LayerInfo): Option[String] =
    Option(info.getMetadata.get(volatileLayerSftHint, classOf[String]))

  def getVolatileAge(info: LayerInfo): Option[Instant] =
    Option(info.getMetadata.get(volatileLayerAgeHint, classOf[JLong]))
}