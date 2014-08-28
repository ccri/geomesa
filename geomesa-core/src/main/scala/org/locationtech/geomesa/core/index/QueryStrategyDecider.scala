/*
 * Copyright 2013-2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.index

import java.util

import org.geotools.data.Query
<<<<<<< HEAD
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Id, PropertyIsLike}

import scala.collection.JavaConversions._
=======
import org.locationtech.geomesa.core.index.AttributeIdxEqualsStrategy
import org.locationtech.geomesa.core.index.QueryHints._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Expression, PropertyName}
import org.opengis.filter.temporal.TEquals
import org.opengis.filter.{Filter, Id, PropertyIsEqualTo, PropertyIsLike}
>>>>>>> jnh_multiTable_fixUnShareTablesTable

object QueryStrategyDecider {

  import org.locationtech.geomesa.core.index.AttributeIndexStrategy.getAttributeIndexStrategy
  import org.locationtech.geomesa.core.index.STIdxStrategy.getSTIdxStrategy

  def chooseStrategy(isCatalogTableFormat: Boolean,
                     sft: SimpleFeatureType,
                     query: Query): Strategy =
    // if datastore doesn't support attr index use spatiotemporal only
    if (isCatalogTableFormat) chooseNewStrategy(sft, query) else new STIdxStrategy

  def chooseNewStrategy(sft: SimpleFeatureType, query: Query): Strategy = {
    val filter = query.getFilter
    val isDensity = query.getHints.containsKey(BBOX_KEY)

    if (isDensity) {
      // TODO GEOMESA-322 use other strategies with density iterator
      new STIdxStrategy
    } else {
      // check if we can use the attribute index first
<<<<<<< HEAD
      val attributeStrategy = AttributeIndexStrategy.getAttributeIndexStrategy(filter, sft)
      attributeStrategy.getOrElse {
        filter match {
          case idFilter: Id => new RecordIdxStrategy
          case and: And => processAnd(isDensity, sft, and)
          case cql          => new STIdxStrategy
        }
      }
    }
  }

  private def processAnd(isDensity: Boolean, sft: SimpleFeatureType, and: And): Strategy = {
    val children: util.List[Filter] = decomposeAnd(and)

    def something(attr: Filter, st: Filter): Strategy = {
      if(children.indexOf(attr) < children.indexOf(st)) getAttributeIndexStrategy(attr, sft).get
      else new STIdxStrategy
    }

    val strats = (children.find(c => getAttributeIndexStrategy(c, sft).isDefined), children.find(c => getSTIdxStrategy(c, sft).isDefined))

    strats match {
      case (Some(attrFilter), Some(stFilter)) => something(attrFilter, stFilter)
      case (None, Some(stFilter)) => new STIdxStrategy
      case (Some(attrFilter), None) => getAttributeIndexStrategy(attrFilter, sft).get  // Never call .get
      case (None, None) => new STIdxStrategy
=======
      val attributeStrategy = getAttributeIndexStrategy(filter, sft)
      attributeStrategy.getOrElse {
        filter match {
          case idFilter: Id => new RecordIdxStrategy
          case cql          => new STIdxStrategy
        }
      }
>>>>>>> jnh_multiTable_fixUnShareTablesTable
    }
  }

  // TODO try to use wildcard values from the Filter itself (https://geomesa.atlassian.net/browse/GEOMESA-309)
  // Currently pulling the wildcard values from the filter
  // leads to inconsistent results...so use % as wildcard
  val MULTICHAR_WILDCARD = "%"
  val SINGLE_CHAR_WILDCARD = "_"
  val NULLBYTE = Array[Byte](0.toByte)

  /* Like queries that can be handled by current reverse index */
  def likeEligible(filter: PropertyIsLike) = containsNoSingles(filter) && trailingOnlyWildcard(filter)

  /* contains no single character wildcards */
  def containsNoSingles(filter: PropertyIsLike) =
    !filter.getLiteral.replace("\\\\", "").replace(s"\\$SINGLE_CHAR_WILDCARD", "").contains(SINGLE_CHAR_WILDCARD)

  def trailingOnlyWildcard(filter: PropertyIsLike) =
    (filter.getLiteral.endsWith(MULTICHAR_WILDCARD) &&
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == filter.getLiteral.length - MULTICHAR_WILDCARD.length) ||
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == -1

<<<<<<< HEAD
=======

  import org.locationtech.geomesa.utils.geotools.Conversions._

  def getAttributeIndexStrategy(f: Filter, sft: SimpleFeatureType): Option[Strategy] =
    f match {
      case filter: PropertyIsEqualTo =>
        checkEqualsExpression(sft, filter.getExpression1, filter.getExpression2)
      case filter: TEquals =>
        checkEqualsExpression(sft, filter.getExpression1, filter.getExpression2)
      case filter: PropertyIsLike =>
        val prop = filter.getExpression.asInstanceOf[PropertyName].getPropertyName
        val indexed = sft.getDescriptor(prop).isIndexed
        if (indexed) Some(new AttributeIdxLikeStrategy) else None
      case _ => None
    }

  private def checkEqualsExpression(sft: SimpleFeatureType, one: Expression, two: Expression): Option[Strategy] = {
    val prop =
      (one, two) match {
        case (p: PropertyName, _) => Some(p.getPropertyName)
        case (_, p: PropertyName) => Some(p.getPropertyName)
        case (_, _)               => None
      }
    prop.filter(p => sft.getDescriptor(p).isIndexed).map(_ => new AttributeIdxEqualsStrategy)
  }
>>>>>>> jnh_multiTable_fixUnShareTablesTable
}
