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
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ContentDataStore
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.kafka.{KafkaDataStoreHelper, ReplayConfig}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.matching.Regex

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
              @DescribeParameter(name = "source workspace", description = "Workspace of Source Store.")
              sourceWorkspace: String,
              @DescribeParameter(name = "store", description = "Name of Source Store")
              sourceStore: String,
              @DescribeParameter(name = "startTime", description = "POSIX Start Time of the replay window.")
              startTime: java.lang.Long,
              @DescribeParameter(name = "endTime", description = "POSIX End Time of the replay window.")
              endTime: java.lang.Long,
              @DescribeParameter(name = "readBehind", description = "The amount of time to pre-read in milliseconds.")
              readBehind: java.lang.Long
              ): String = {

    val sourceWorkSpaceInfo: WorkspaceInfo = getWorkSpace(sourceWorkspace)

    val sourceStoreInfo: DataStoreInfo = Option(catalog.getDataStoreByName(sourceWorkSpaceInfo.getName, sourceStore)).getOrElse {
      throw new ProcessException(s"Unable to find store $sourceStore in source workspace $sourceWorkspace")
    }
    // set the catalogBuilder to our store
    val catalogBuilder = new CatalogBuilder(catalog)
    catalogBuilder.setWorkspace(sourceWorkSpaceInfo)
    catalogBuilder.setStore(sourceStoreInfo)

    // parse time parameters and make Replay Config
    val replayConfig = new ReplayConfig(startTime, endTime, readBehind)

    // create volatile SFT
    val volatileSFT = createVolatileSFT(features, sourceStoreInfo, replayConfig)
    // update store to save new metadata
    catalog.save(sourceStoreInfo)

    // check for existing layer
    if (checkForLayer(sourceWorkSpaceInfo.getName, volatileSFT.getTypeName))
      throw new ProcessException(s"Target layer already exists for SFT: ${volatileSFT.getTypeName}")

    // add a well known volatile keyword
    val volatileTypeInfo = catalogBuilder.buildFeatureType(volatileSFT.getName)

    // do some setup
    catalogBuilder.setupBounds(volatileTypeInfo)

    // build the layer and mark as volatile
    val volatileLayerInfo = catalogBuilder.buildLayer(volatileTypeInfo)
    // add the name of the volatile SFT associated with this new layer
    volatileLayerInfo.getMetadata.put(volatileHint, volatileHint)
    volatileLayerInfo.getMetadata.put(volatileLayerSftHint, volatileSFT.getTypeName)
    // Add new layer with hints to GeoServer
    catalog.add(volatileTypeInfo)
    catalog.add(volatileLayerInfo)

    s"created layer: ${volatileLayerInfo.getName}"
  }

  private def createVolatileSFT(features: SimpleFeatureCollection,
                                storeInfo: DataStoreInfo,
                                rConfig: ReplayConfig): SimpleFeatureType = {

    val ds = storeInfo.getDataStore(null).asInstanceOf[ContentDataStore]

    // go back to the DS to get the schema to ensure that it contains all required user data
    val extantSFT: SimpleFeatureType = ds.getSchema(features.getSchema.getTypeName)

    val replaySFT: SimpleFeatureType = KafkaDataStoreHelper.prepareForReplay(extantSFT, rConfig)
    ds.createSchema(replaySFT)
    injectAge(storeInfo, replaySFT)
    //verify by retrieving the stored sft
    val storedSFT = ds.getSchema(replaySFT.getName)
    storedSFT
  }
  
  private def getWorkSpace(ws: String): WorkspaceInfo = Option(catalog.getWorkspaceByName(ws)) match {
    case Some(wsi) => wsi
    case _         => catalog.getDefaultWorkspace
  }

  private def checkForLayer(workspace: String, sftTypeName: String): Boolean = {
    val layerName = s"$workspace:$sftTypeName"
    val layer = catalog.getLayerByName(layerName)
    if (layer == null) false else true
  }

}

object ReplayKafkaDataStoreProcess {
  val volatileLayerSftHint: String = "kafka.geomesa.volatile.layer.sft"
  val volatileSftAgeHint: String   = "kafka.geomesa.volatile.age.of.sft "
  val volatileSftPattern: Regex = s"($volatileSftAgeHint)(.*)".r
  val volatileHint: String = "kafka.geomesa.volatile"
  val volatileKW: Keyword = new Keyword(volatileHint)

  private def makeAgeKey(sfts: String): String = volatileSftAgeHint + sfts
  private def makeAgeKey(sft: SimpleFeatureType): String = makeAgeKey(sft.getTypeName)

  implicit def longToInstant(l: JLong): Instant = new Instant(l)
  implicit def longToDuration(l: JLong): Duration = new Duration(l)

  def injectAge(dsi: DataStoreInfo, sft: SimpleFeatureType): Unit = {
    dsi.getMetadata.put(makeAgeKey(sft), System.currentTimeMillis().asInstanceOf[JLong]) }

  def getSftFromKey(key: String): Option[String] = key match {
    case volatileSftPattern(hint, sftname) => Some(sftname)
    case _                                 => None
  }

  def getVolatileAge(dsi: DataStoreInfo, sft: SimpleFeatureType): Option[Instant] = {
    getVolatileAge(dsi, sft.getTypeName)
  }

  def getVolatileAge(dsi: DataStoreInfo, sfts: String): Option[Instant] = {
    val meta = dsi.getMetadata
    val ageKey = makeAgeKey(sfts)
    meta.containsKey(ageKey) match {
      case true => Some(meta.get(ageKey, classOf[JLong]))
      case _    => None
    }
  }
}