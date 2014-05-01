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

package geomesa.core.data.mapreduce

import geomesa.core.{VersionSpecificOperations, DEFAULT_FEATURE_NAME}
import geomesa.core.data.{HLWTKVMapper, AccumuloDataStoreFactoryHelper, MapReduceAccumuloDataStore}
import geomesa.utils.geotools.FeatureHandler
import geomesa.utils.text.WKBUtils
import org.apache.hadoop.io.{Text, LongWritable}
import org.geotools.data.{Base64, DataUtilities, DataStoreFinder, FeatureWriter}
import org.geotools.factory.Hints
import org.geotools.filter.identity.FeatureIdImpl
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import com.typesafe.scalalogging.slf4j.Logging

abstract class AbstractFeatureIngestMapperWrapper(ops: VersionSpecificOperations) {

  abstract class AbstractFeatureIngestMapper extends HLWTKVMapper with Logging {

    var featureType: SimpleFeatureType = null
    var fw: FeatureWriter[SimpleFeatureType, SimpleFeature] = null

    override def setup(context: HLWTKVMapper#Context) {
      super.setup(context)

      val featureName = context.getConfiguration.get(DEFAULT_FEATURE_NAME)
      val ds = DataStoreFinder.getDataStore(
        AccumuloDataStoreFactoryHelper.getMRAccumuloConnectionParams(
          context.getConfiguration)).asInstanceOf[MapReduceAccumuloDataStore]

      featureType = ds.getSchema(featureName)
      fw = ds.createMapReduceFeatureWriter(featureName, context)
    }

    override def map(key: LongWritable, value: Text, context: HLWTKVMapper#Context) {
      val geom::encoded = value.toString.split(FeatureHandler.OUTPUT_FIELD_SEPARATOR_CHAR).toList

      try {
        // decode the simple-feature
        // ("mkString" is here to guard against strings broken by our separator)
        val simpleFeature = DataUtilities.createFeature(featureType,
          encoded.mkString(FeatureHandler.OUTPUT_FIELD_SEPARATOR))
        simpleFeature.setDefaultGeometry(WKBUtils.read(Base64.decode(geom)))
        simpleFeature.getUserData.put(Hints.USE_PROVIDED_FID, Boolean.box(x = true))

        val next = fw.next()
        // "Clone" the simpleFeature to write into the featureWriter's "next" and then write it to Accumulo.
        next.setAttributes(simpleFeature.getAttributes)
        next.getIdentifier.asInstanceOf[FeatureIdImpl].setID(simpleFeature.getID)

        fw.write()
      } catch {
        case e: Exception => logger.warn("Problem writing feature; skipping it.", e)
      }
    }
  }
}