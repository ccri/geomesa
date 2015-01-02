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

package org.locationtech.geomesa.core.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.core.stats.MethodProfiling

/**
 * This is an Index Only Iterator, to be used in situations where the data records are
 * not useful enough to pay the penalty of decoding when using the
 * SpatioTemporalIntersectingIterator.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class IndexIterator
    extends GeomesaFilteringIterator
    with HasFeatureBuilder
    with HasIndexValueDecoder
    with HasSpatioTemporalFilter
    with HasFeatureDecoder
    with HasTransforms
    with HasInMemoryDeduplication
    with MethodProfiling
    with Logging {

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)
  }

  override def setTopConditionally() = {
    // TODO possible merge error, the below was in the rc4 merge
//    TServerClassLoader.initClassLoader(logger)
//
//    val simpleFeatureTypeSpec = options.get(GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
//
//    val featureType = SimpleFeatureTypes.createType(this.getClass.getCanonicalName, simpleFeatureTypeSpec)
//    featureType.decodeUserData(options, GEOMESA_ITERATORS_SIMPLE_FEATURE_TYPE)
//
//    indexEncoder = IndexValueEncoder(featureType)
//
//    dateAttributeName = getDtgFieldName(featureType)
//
//    // default to text if not found for backwards compatibility
//    val encodingOpt = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
//    featureEncoder = SimpleFeatureEncoder(featureType, encodingOpt)
//
//    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(featureType)
//
//    // TODO not in 1.5
////    val schemaEncoding = options.get(DEFAULT_SCHEMA_NAME)
////    decoder = IndexSchema.getIndexEntryDecoder(schemaEncoding)
//
//    if (options.containsKey(DEFAULT_FILTER_PROPERTY_NAME)) {
//      val filterString  = options.get(DEFAULT_FILTER_PROPERTY_NAME)
//      filter = ECQL.toFilter(filterString)
//      val sfb = new SimpleFeatureBuilder(featureType)
//      testSimpleFeature = sfb.buildFeature("test")
//    }
//
//    if (options.containsKey(DEFAULT_CACHE_SIZE_NAME))
//      maxInMemoryIdCacheEntries = options.get(DEFAULT_CACHE_SIZE_NAME).toInt
//    deduplicate = IndexSchema.mayContainDuplicates(featureType)
//
//    this.indexSource = source.deepCopy(env)
//  }
//
//  /**
//   * Generates from the key's value a SimpleFeature that matches the current
//   * (top) reference of the index-iterator.
//   *
//   * We emit the top-key from the index-iterator, and the top-value from the
//   * converted key value.  This is *IMPORTANT*, as otherwise we do not emit rows
//   * that honor the SortedKeyValueIterator expectation, and Bad Things Happen.
//   */
//  override def seekData(decodedValue: DecodedIndexValue) {
//    // now increment the value of nextKey, copy because reusing it is UNSAFE
//    nextKey = new Key(indexSource.getTopKey)
//    // using the already decoded index value, generate a SimpleFeature and set as the Value
//    val nextSimpleFeature = IndexIterator.encodeIndexValueToSF(featureBuilder, decodedValue)
////    val nextSimpleFeature = IndexIterator.encodeIndexValueToSF(featureBuilder, decodedValue.id,
////      decodedValue.geom, decodedValue.dtgMillis) // TODO not in 1.5
//    nextValue = new Value(featureEncoder.encode(nextSimpleFeature))
//  }
//
//  override def deepCopy(env: IteratorEnvironment) =
//    throw new UnsupportedOperationException("IndexIterator does not support deepCopy.")
//}

    val indexKey = source.getTopKey

    if (!SpatioTemporalTable.isIndexEntry(indexKey)) {
      // if this is a data entry, skip it
      logger.warn("Found unexpected data entry: " + indexKey)
    } else {
      // the value contains the full-resolution geometry and time plus feature ID
      val decodedValue = indexEncoder.decode(source.getTopValue.get)

      // evaluate the filter checks, in least to most expensive order
      val meetsIndexFilters = checkUniqueId.forall(fn => fn(decodedValue.id)) &&
          stFilter.forall(fn => fn(decodedValue.geom, decodedValue.date.map(_.getTime)))

      if (meetsIndexFilters) { // we hit a valid geometry, date and id
        val transformedFeature = encodeIndexValueToSF(decodedValue)
        // update the key and value
        // copy the key because reusing it is UNSAFE
        topKey = Some(indexKey)
        topValue = transform.map(fn => new Value(fn(transformedFeature)))
            .orElse(Some(new Value(featureEncoder.encode(transformedFeature))))
      }
    }
  }
}
