/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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


package geomesa.core.data

import geomesa.core.VersionSpecificOperations
import geomesa.core.index._
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.data.{Mutation, Value, Key}
import org.apache.hadoop.mapred.{Reporter, RecordWriter}
import org.geotools.data.simple.SimpleFeatureWriter
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry

object AccumuloFeatureWriter {

  type AccumuloRecordWriter = RecordWriter[Key,Value]

  class LocalRecordWriter(tableName: String,
                          connector: Connector,
                          ops: VersionSpecificOperations) extends AccumuloRecordWriter {
    private val bw = ops.createBatchWriter(connector, tableName)

    def write(key: Key, value: Value) {
      val m = new Mutation(key.getRow)
      m.put(key.getColumnFamily, key.getColumnQualifier, key.getTimestamp, value)
      bw.addMutation(m)
    }

    def close(reporter: Reporter) {
      bw.flush()
      bw.close()
    }
  }

  class MapReduceRecordWriter(context: HLWTKVMapper#Context) extends AccumuloRecordWriter {
    def write(key: Key, value: Value) {
      context.write(key, value)
    }

    def close(reporter: Reporter) {}
  }
}

class AccumuloFeatureWriter(featureType: SimpleFeatureType,
                            indexer: SpatioTemporalIndexSchema,
                            recordWriter: RecordWriter[Key,Value])
    extends SimpleFeatureWriter with Logging {

  var currentFeature: SimpleFeature = null

  val builder = new SimpleFeatureBuilder(featureType)

  def getFeatureType: SimpleFeatureType = featureType

  def remove() {}

  def write() {

    // require a non-null feature with a non-null geometry
    if (currentFeature != null && currentFeature.getDefaultGeometry != null) {

      // see if there's a suggested ID to use for this feature
      // (relevant when this insertion is wrapped inside a Transaction)
      if (currentFeature.getUserData.containsKey(Hints.PROVIDED_FID)) {
        builder.init(currentFeature)
        currentFeature = builder.buildFeature(
          currentFeature.getUserData.get(Hints.PROVIDED_FID).toString)
      }

      indexer.encode(currentFeature).foreach{ case (k,v) => recordWriter.write(k,v) }

    } else logger.warn(s"Invalid feature to write:  $currentFeature")

    currentFeature = null
  }

  def hasNext: Boolean = false

  def next(): SimpleFeature = {
    currentFeature = SimpleFeatureBuilder.template(featureType, "")
    currentFeature
  }

  def close() {
    recordWriter.close(null)
  }
}