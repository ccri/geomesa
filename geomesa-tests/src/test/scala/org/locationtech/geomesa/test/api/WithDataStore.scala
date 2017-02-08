package org.locationtech.geomesa.test.api

import org.geotools.data.FeatureStore
import org.geotools.data.simple.SimpleFeatureStore
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait WithDataStore {
  def spec: String
  def addFeatures(features: Seq[SimpleFeature])
  def sft: SimpleFeatureType
  def fs: SimpleFeatureStore
}
