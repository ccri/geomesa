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

import java.io.Serializable
import java.util.{Map => JMap}
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import com.typesafe.scalalogging.slf4j.Logging

import scala.collection.JavaConversions._
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.{params => dsp}

trait RasterIngest extends Logging {
  def getAccumuloCoverageStoreConf(config: Map[String, Option[String]]): JMap[String, Serializable] =
    mapAsJavaMap(Map(
      dsp.instanceIdParam.getName   -> config(IngestRasterParams.ACCUMULO_INSTANCE).get,
      dsp.zookeepersParam.getName   -> config(IngestRasterParams.ZOOKEEPERS).get,
      dsp.userParam.getName         -> config(IngestRasterParams.ACCUMULO_USER).get,
      dsp.passwordParam.getName     -> config(IngestRasterParams.ACCUMULO_PASSWORD).get,
      dsp.tableNameParam.getName    -> config(IngestRasterParams.TABLE).get,
      dsp.geoserverParam.getName    -> config(IngestRasterParams.GEOSERVER_REG),
      dsp.authsParam.getName        -> config(IngestRasterParams.AUTHORIZATIONS),
      dsp.visibilityParam.getName   -> config(IngestRasterParams.VISIBILITIES),
      dsp.shardsParam.getName       -> config(IngestRasterParams.SHARDS),
      dsp.writeMemoryParam.getName  -> config(IngestRasterParams.WRITE_MEMORY),
      dsp.writeThreadsParam         -> config(IngestRasterParams.WRITE_THREADS),
      dsp.queryThreadsParam.getName -> config(IngestRasterParams.QUERY_THREADS)
    ).collect {
      case (key, Some(value)) => (key, value);
      case (key, value: String) => (key, value)
    }).asInstanceOf[java.util.Map[String, Serializable]]

  def createCoverageStore(config: Map[String, Option[String]]): AccumuloCoverageStore = {
    val rasterName = config(IngestRasterParams.RASTER_NAME)
    if (rasterName == null || rasterName.isEmpty) {
      logger.error("No raster name specified for raster feature ingest." +
        " Please check that all arguments are correct in the previous command. ")
      sys.exit()
    }

    val csConfig: JMap[String, Serializable] = getAccumuloCoverageStoreConf(config)

    AccumuloCoverageStore(csConfig)
  }
}
