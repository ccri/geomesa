/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.tables.AttributeTable._
import org.opengis.feature.`type`.AttributeDescriptor

import scala.util.{Failure, Success}

/**
 * This is an Attribute Index Only Iterator. It should be used to avoid a join on the records table
 * in cases where only the geom, dtg and attribute in question are needed.
 *
 * This iterator returns as its nextKey the key for the index. nextValue is
 * the value for the INDEX, mapped into a SimpleFeature
 */
class AttributeIndexIterator
    extends GeomesaFilteringIterator
    with HasFeatureBuilder
    with HasIndexValueDecoder
    with HasFeatureDecoder
    with HasSpatioTemporalFilter
    with HasTransforms
    with Logging {

  // the following fields get filled in during init
  var attributeRowPrefix: String = null
  var attributeType: Option[AttributeDescriptor] = null

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment) {
    super.init(source, options, env)
    initFeatureType(options)
    init(featureType, options)

    attributeRowPrefix = index.getTableSharingPrefix(featureType)
    // if we're retrieving the attribute, we need the class in order to decode it
    attributeType = Option(options.get(GEOMESA_ITERATORS_ATTRIBUTE_NAME))
        .flatMap(n => Option(featureType.getDescriptor(n)))

    // TODO possible merge error, the below was in the rc4 merge
////        .map(_.getType.getBinding) // TODO not in 1.5
//
//    // default to text if not found for backwards compatibility
//    val encoding = Option(options.get(FEATURE_ENCODING)).getOrElse(FeatureEncoding.TEXT.toString)
//    featureEncoder = SimpleFeatureEncoder(featureType, encoding)
//
//    featureBuilder = AvroSimpleFeatureFactory.featureBuilder(featureType)
//
//    // combine the simpleFeatureFilteringIterator functionality so we only have to decode each row once
//    Option(options.get(DEFAULT_FILTER_PROPERTY_NAME)).foreach { filterString =>
//      val filter = ECQL.toFilter(filterString)
//
//      val sfb = new SimpleFeatureBuilder(featureType)
//      val testFeature = sfb.buildFeature("test")
//
//      filterTest = (geom: Geometry, odate: Option[Long]) => {
//        testFeature.setDefaultGeometry(geom)
//        dtgFieldName.foreach(dtgField => odate.foreach(date => testFeature.setAttribute(dtgField, new Date(date))))
//        filter.evaluate(testFeature)
//      }
//    }
//
//    this.indexSource = source.deepCopy(env)
//  }
//
//  override def hasTop = topKey.isDefined
//
//  override def getTopKey = topKey.orNull
//
//  override def getTopValue = topValue.orNull
//
//  /**
//   * Seeks to the start of a range and fetches the top key/value
//   *
//   * @param range
//   * @param columnFamilies
//   * @param inclusive
//   */
//  override def seek(range: Range, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean) {
//    // move the source iterator to the right starting spot
//    indexSource.seek(range, columnFamilies, inclusive)
//    findTop()
  }

  override def setTopConditionally() {

    // the value contains the full-resolution geometry and time
    lazy val decodedValue = indexEncoder.decode(source.getTopValue.get)

    // evaluate the filter check
    val meetsIndexFilters =
        stFilter.forall(fn => fn(decodedValue.geom, decodedValue.date.map(_.getTime)))

    if (meetsIndexFilters) {
      // current entry matches our filter - update the key and value
      topKey = Some(source.getTopKey)
      // using the already decoded index value, generate a SimpleFeature
      val sf = encodeIndexValueToSF(decodedValue)

      // if they requested the attribute value, decode it from the row key
      if (attributeType.isDefined) {
        val row = topKey.get.getRow.toString
        val decoded = decodeAttributeIndexRow(attributeRowPrefix, attributeType.get, row)
        decoded match {
          case Success(att) => sf.setAttribute(att.attributeName, att.attributeValue)
          case Failure(e) => logger.error(s"Error decoding attribute row: row: $row, error: ${e.toString}")

      // TODO possible merge error, the below was in the rc4 merge
//      if (filterTest(decodedValue.geom, decodedValue.date.map(_.getTime))) {
//        // current entry matches our filter - update the key and value
//        // copy the key because reusing it is UNSAFE
//        topKey = Some(new Key(indexSource.getTopKey))
//        // using the already decoded index value, generate a SimpleFeature
//        val sf = IndexIterator.encodeIndexValueToSF(featureBuilder, decodedValue)
////        val sf = IndexIterator.encodeIndexValueToSF(featureBuilder,
////                                                    decodedValue.id,
////                                                    decodedValue.geom,
////                                                    decodedValue.dtgMillis) // TODO not in 1.5
//
//        // if they requested the attribute value, decode it from the row key
//        if (attributeType.isDefined) {
//          val row = topKey.get.getRow.toString
//          val decoded = decodeAttributeIndexRow(attributeRowPrefix, attributeType.get, row)
//          decoded match {
//            case Success(att) => sf.setAttribute(att.attributeName, att.attributeValue)
//            case Failure(e) => logger.error(s"Error decoding attribute row: row: $row, error: ${e.toString}")
//          }
        }
      }

      // set the encoded simple feature as the value
      topValue = transform.map(fn => new Value(fn(sf))).orElse(Some(new Value(featureEncoder.encode(sf))))
    }
  }
}


