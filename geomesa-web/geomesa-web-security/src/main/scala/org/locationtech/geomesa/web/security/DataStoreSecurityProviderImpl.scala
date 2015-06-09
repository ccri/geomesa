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
package org.locationtech.geomesa.web.security

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.geoserver.security.decorators.{DecoratingDataAccess, DecoratingDataStore, DecoratingSimpleFeatureSource}
import org.geotools.data._
import org.geotools.data.simple.{SimpleFeatureCollection, SimpleFeatureSource}
import org.geotools.feature.FeatureCollection
import org.geotools.feature.collection.FilteringSimpleFeatureCollection
import org.locationtech.geomesa.security.{DataStoreSecurityProvider, VisibilityFilter}
import org.locationtech.geomesa.web.security.DataStoreSecurityProviderImpl.{DA, FC, FR, FS}
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/** Implementation of [[DataStoreSecurityProvider]] using the spring security context to access the
  * user's authorizations.
  */
class DataStoreSecurityProviderImpl extends DataStoreSecurityProvider with LazyLogging {

  override def secure(fs: FS): FS = GMSecureFeatureSource(fs)

  override def secure(fr: FR): FR = GMSecureFeatureReader(fr)

  override def secure(fc: FC): FC = GMSecureFeatureCollection(fc)
}

object DataStoreSecurityProviderImpl {
  type DA = DataAccess[SimpleFeatureType, SimpleFeature]
  type FS = FeatureSource[SimpleFeatureType, SimpleFeature]
  type FR = FeatureReader[SimpleFeatureType, SimpleFeature]
  type FC = FeatureCollection[SimpleFeatureType, SimpleFeature]
}

class GMSecureDataAccess(delegate: DA)
  extends DecoratingDataAccess[SimpleFeatureType, SimpleFeature](delegate)
  with LazyLogging {

  logger.info("Secured Data Access '{}'", delegate)

  override def getFeatureSource(typeName: Name): SimpleFeatureSource =
    GMSecureFeatureSource(super.getFeatureSource(typeName), this)
}

class GMSecureDataStore(delegate: DataStore) extends DecoratingDataStore(delegate) with LazyLogging {
  
  logger.info("Secured Data Store '{}'", delegate)

  override def getFeatureSource(typeName: Name): SimpleFeatureSource =
    new GMSecureFeatureSource(super.getFeatureSource(typeName), this)

  override def getFeatureSource(typeName: String): SimpleFeatureSource =
    new GMSecureFeatureSource(super.getFeatureSource(typeName), this)

  override def getFeatureReader(query: Query, transaction: Transaction): FR =
    GMSecureFeatureReader(super.getFeatureReader(query, transaction))
}

class GMSecureFeatureSource(delegate: SimpleFeatureSource, secureDataStore: DA)
  extends DecoratingSimpleFeatureSource(delegate)
  with LazyLogging {

  logger.info("Secured Feature Source '{}'", delegate)

  override val getDataStore: DA =
    secureDataStore

  override def getFeatures: SimpleFeatureCollection =
    GMSecureFeatureCollection(super.getFeatures)

  override def getFeatures(filter: Filter): SimpleFeatureCollection =
    GMSecureFeatureCollection(super.getFeatures(filter))

  override def getFeatures(query: Query): SimpleFeatureCollection =
    GMSecureFeatureCollection(super.getFeatures(query))
}

object GMSecureFeatureSource {

  def apply(delegate: FS, secureDataAccess: GMSecureDataAccess): GMSecureFeatureSource =
    new GMSecureFeatureSource(DataUtilities.simple(delegate), secureDataAccess)

  def apply(delegate: FS): GMSecureFeatureSource = {
    val secureDataAccess = delegate.getDataStore match {
      case ds: DataStore => new GMSecureDataStore(ds)
      case da => new GMSecureDataAccess(da)
    }
    new GMSecureFeatureSource(DataUtilities.simple(delegate), secureDataAccess)
  }
}

object GMSecureFeatureCollection extends LazyLogging {

  def apply(fc: FC): SimpleFeatureCollection = {
    logger.info("Secured Feature Collection '{}'", fc)

    val filter = VisibilityFilter()
    new FilteringSimpleFeatureCollection(fc, filter)
  }
}

object GMSecureFeatureReader extends LazyLogging {

  def apply(fr: FR): FR = {
    logger.info("Secured Feature Reader '{}'", fr)

    val filter = VisibilityFilter()
    new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](fr, filter)
  }
}

