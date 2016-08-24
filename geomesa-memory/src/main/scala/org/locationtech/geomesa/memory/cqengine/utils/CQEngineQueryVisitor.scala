/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.memory.cqengine.utils

import java.util.Date

import com.googlecode.cqengine.attribute.Attribute
import com.googlecode.cqengine.query.Query
import com.googlecode.cqengine.{query => cqquery}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.filter.visitor.AbstractFilterVisitor
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.memory.cqengine.attribute.SimpleFeatureAttribute
import org.locationtech.geomesa.memory.cqengine.query.{CQLQuery, Intersects => CQIntersects}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial._
import org.opengis.filter.temporal._
import org.opengis.temporal.Period

import scala.collection.JavaConversions._
import scala.language._

class CQEngineQueryVisitor(sft: SimpleFeatureType) extends AbstractFilterVisitor {
  implicit val lookup = SFTAttributes(sft)

  override def visit(filter: And, data: scala.Any): AnyRef = {
    val children = filter.getChildren

    val query = children.map { f =>
      f.accept(this, null) match {
        case q: Query[SimpleFeature] => q
        case _ => throw new Exception(s"Filter visitor didn't recognize filter: $f.")
      }
    }.toList
    new cqquery.logical.And[SimpleFeature](query)
  }

  override def visit(filter: Or, data: scala.Any): AnyRef = {
    val children = filter.getChildren

    val query = children.map { f =>
      f.accept(this, null) match {
        case q: Query[SimpleFeature] => q
        case _ => throw new Exception(s"Filter visitor didn't recognize filter: $f.")
      }
    }.toList
    new cqquery.logical.Or[SimpleFeature](query)
  }

  override def visit(filter: Not, data: scala.Any): AnyRef = {
    val subfilter = filter.getFilter

    val subquery = subfilter.accept(this, null) match {
      case q: Query[SimpleFeature] => q
      case _ => throw new Exception(s"Filter visitor didn't recognize filter: $subfilter.")
    }
    new cqquery.logical.Not[SimpleFeature](subquery)
  }

  override def visit(filter: BBOX, data: scala.Any): AnyRef = {
    val attributeName = filter.getExpression1.asInstanceOf[PropertyName].getPropertyName
    val geom = filter.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])

    val geomAttribute = lookup.lookup[Geometry](attributeName)

    new CQIntersects(geomAttribute, geom)
  }

  override def visit(filter: Intersects, data: scala.Any): AnyRef = {
    val attributeName = filter.getExpression1.asInstanceOf[PropertyName].getPropertyName
    val geom = filter.getExpression2.asInstanceOf[Literal].evaluate(null, classOf[Geometry])

    val geomAttribute = lookup.lookup[Geometry](attributeName)

    new CQIntersects(geomAttribute, geom)
  }

  override def visit(filter: PropertyIsEqualTo, data: scala.Any): AnyRef = {
    val (attribute: Attribute[SimpleFeature, Any], value: Any) = extractAttributeAndValue(filter)
    new cqquery.simple.Equal(attribute, value)
  }

  override def visit(filter: PropertyIsNull, data: scala.Any): AnyRef = {
    val attributeName = filter.getExpression.asInstanceOf[PropertyName].getPropertyName
    val attr = lookup.lookup[Any](attributeName)
    // ugh
    new cqquery.logical.Not[SimpleFeature](new cqquery.simple.Has(attr))
  }

  /**
    * name BETWEEN lower AND upper
    * (in the OpenGIS spec, lower and upper are inclusive)
   */
  override def visit(filter: PropertyIsBetween, data: scala.Any): AnyRef = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntBetweenQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongBetweenQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatBetweenQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleBetweenQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateBetweenQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsBetween: $c not supported")
    }
  }

  override def visit(filter: PropertyIsGreaterThan, data: scala.Any): cqquery.simple.GreaterThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsGreaterThan: $c not supported")
    }
  }

  override def visit(filter: PropertyIsGreaterThanOrEqualTo, data: scala.Any): cqquery.simple.GreaterThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTEQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTEQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTEQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTEQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateGTEQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsGreaterThanOrEqualTo: $c not supported")
    }
  }

  override def visit(filter: PropertyIsLessThan, data: scala.Any): cqquery.simple.LessThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsLessThan: $c not supported")
    }
  }

  override def visit(filter: PropertyIsLessThanOrEqualTo, data: scala.Any): cqquery.simple.LessThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTEQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTEQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTEQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTEQuery(prop)
      case c if classOf[java.util.Date   ].isAssignableFrom(c) => BuildDateLTEQuery(prop)
      case c => throw new RuntimeException(s"PropertyIsLessThanOrEqualTo: $c not supported")
    }
  }

  /**
    * AFTER: only for time attributes, and is exclusive
    */
  override def visit(after: After, extraData: scala.Any): AnyRef = {
    val prop = getAttributeProperty(after).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[Date](prop.name)
        val value = prop.literal.evaluate(null, classOf[Date])
        new cqquery.simple.GreaterThan[SimpleFeature, Date](attr, value, false)
      }
      case c => throw new RuntimeException(s"After: $c not supported")
    }
  }

  /**
    * BEFORE: only for time attributes, and is exclusive
    */
  override def visit(before: Before, extraData: scala.Any): AnyRef = {
    val prop = getAttributeProperty(before).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[Date](prop.name)
        val value = prop.literal.evaluate(null, classOf[Date])
        new cqquery.simple.LessThan[SimpleFeature, Date](attr, value, false)
      }
      case c => throw new RuntimeException(s"Before: $c not supported")
    }
  }

  /**
    * DURING: only for time attributes, and is exclusive at both ends
    */
  override def visit(during: During, extraData: scala.Any): AnyRef = {
    val prop = getAttributeProperty(during).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[Date].isAssignableFrom(c) => {
        val attr = lookup.lookupComparable[Date](prop.name)
        val p = prop.literal.evaluate(null, classOf[Period])
        val lower = p.getBeginning.getPosition.getDate
        val upper = p.getEnding.getPosition.getDate
        new cqquery.simple.Between[SimpleFeature, java.util.Date](attr, lower, false, upper, false)
      }
      case c => throw new RuntimeException(s"During: $c not supported")
    }
  }

  override def visit(filter: BinaryComparisonOperator, data: scala.Any): AnyRef = ???

  override def visit(filter: ExcludeFilter, data: scala.Any): AnyRef = ???

  override def visit(filter: IncludeFilter, data: scala.Any): AnyRef = ???

  override def visit(filter: Contains, data: scala.Any): AnyRef = ???

  override def visit(filter: Crosses, data: scala.Any): AnyRef = handleGeneralCQLFilter(filter)

  override def visit(filter: Disjoint, data: scala.Any): AnyRef = ???

  override def visit(filter: DWithin, data: scala.Any): AnyRef = ???

  override def visit(filter: Equals, data: scala.Any): AnyRef = ???

  override def visit(filter: Overlaps, data: scala.Any): AnyRef = ???

  override def visit(filter: Touches, data: scala.Any): AnyRef = ???

  override def visit(filter: Within, data: scala.Any): AnyRef = ???

  override def visit(filter: PropertyIsLike, data: scala.Any): AnyRef = ???

  override def visit(meets: Meets, extraData: scala.Any): AnyRef = ???

  override def visit(metBy: MetBy, extraData: scala.Any): AnyRef = ???

  override def visit(overlappedBy: OverlappedBy, extraData: scala.Any): AnyRef = ???

  override def visit(contains: TContains, extraData: scala.Any): AnyRef = ???

  override def visit(equals: TEquals, extraData: scala.Any): AnyRef = ???

  override def visit(contains: TOverlaps, extraData: scala.Any): AnyRef = ???

  override def visit(filter: BinaryTemporalOperator, data: scala.Any): AnyRef = ???

  override def visit(anyInteracts: AnyInteracts, extraData: scala.Any): AnyRef = ???

  override def visit(filter: PropertyIsNil, extraData: scala.Any): AnyRef = ???

  override def visit(filter: Beyond, data: scala.Any): AnyRef = ???

  override def visit(filter: BinarySpatialOperator, data: scala.Any): AnyRef = ???

  override def visit(ends: Ends, extraData: scala.Any): AnyRef = ???

  override def visit(endedBy: EndedBy, extraData: scala.Any): AnyRef = ???

  override def visit(begunBy: BegunBy, extraData: scala.Any): AnyRef = ???

  override def visit(begins: Begins, extraData: scala.Any): AnyRef = ???

  override def visit(filter: PropertyIsNotEqualTo, data: scala.Any): AnyRef = ???

  override def visit(filter: BinaryLogicOperator, data: scala.Any): AnyRef = ???

  override def visitNullFilter(data: scala.Any): AnyRef = ???

  def handleGeneralCQLFilter(filter: Filter): Query[SimpleFeature] = {
    // JNH: HAHAHHAHAHHAHAHA WHWHHAHHAHAHAHAHAHAH!!!!11!!!!
    val attribute = new SimpleFeatureAttribute(classOf[String], "foo")
//    val (attribute: Attribute[SimpleFeature, Any], value: Any) = extractAttributeAndValue(filter)
    new CQLQuery(attribute, filter)
  }

  def extractAttributeAndValue(filter: Filter): (Attribute[SimpleFeature, Any], Any) = {
    val prop = getAttributeProperty(filter).get
    val attributeName = prop.name
    val attribute = lookup.lookup[Any](attributeName)
    val value = prop.literal.evaluate(null, attribute.getAttributeType)
    (attribute, value)
  }

  // JNH: TODO: revisit if this this needed.
  override def toString = "CQEngineQueryVisit()"
}
