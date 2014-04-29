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

import geomesa.core
import geomesa.core.data.AccumuloFeatureWriter.{LocalRecordWriter, MapReduceRecordWriter}
import geomesa.core.index.{Constants, SpatioTemporalIndexSchema}
import java.io.Serializable
import java.util.{Map=>JMap}
import org.apache.accumulo.core.client.mock.MockConnector
import org.apache.accumulo.core.client.{BatchWriter, BatchWriterConfig, IteratorSetting, Connector}
import org.apache.accumulo.core.data.{Key, Mutation, Value, Range}
import org.apache.accumulo.core.file.keyfunctor.ColumnFamilyFunctor
import org.apache.accumulo.core.iterators.user.VersioningIterator
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.data._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.Hints
import org.geotools.feature.NameImpl
import org.geotools.geometry.jts.ReferencedEnvelope
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.referencing.crs.CoordinateReferenceSystem
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import geomesa.core.data.FeatureEncoding.FeatureEncoding

/**
 *
 * @param connector        Accumulo connector
 * @param tableName        The name of the Accumulo table contains the various features
 * @param authorizations   The authorizations used to access data
 *
 * This class handles DataStores which are stored in Accumulo Tables.  To be clear, one table may contain multiple
 * features addressed by their featureName.
 */
class AccumuloDataStore(override val connector: Connector,
                        override val tableName: String,
                        override val authorizations: Authorizations,
                        override val indexSchemaFormat: String = "DEFAULT",
                        override val featureEncoding: FeatureEncoding = FeatureEncoding.AVRO)
  extends AbstractAccumuloDataStore(connector,
                                    tableName,
                                    authorizations,
                                    indexSchemaFormat,
                                    featureEncoding) {

  override protected def createBatchWriter(tableName: String, maxMemory: Long, maxWriteThreads: Int) = {
    val batchWriterConfig = new BatchWriterConfig().setMaxMemory(maxMemory)
                                                   .setMaxWriteThreads(maxWriteThreads)
    connector.createBatchWriter(tableName, batchWriterConfig)
  }

}

/**
 *
 * @param connector        Accumulo connector
 * @param tableName        The name of the Accumulo table contains the various features
 * @param authorizations   The authorizations used to access data
 * @param params           The parameters used to create this datastore.
 *
 * This class provides an additional writer which can be accessed by
 *  createMapReduceFeatureWriter(featureName, context)
 *
 * This writer is appropriate for use inside a MapReduce job.  We explicitly do not override the default
 * createFeatureWriter so that we have both available.
 */
class MapReduceAccumuloDataStore(connector: Connector,
                                 tableName: String,
                                 authorizations: Authorizations,
                                 override val params: JMap[String, Serializable],
                                 indexSchemaFormat: String = "DEFAULT",
                                 featureEncoding: FeatureEncoding = FeatureEncoding.AVRO)
  extends AccumuloDataStore(connector, tableName, authorizations, indexSchemaFormat, featureEncoding)
      with AbstractMapReduceAccumuloDataStore


