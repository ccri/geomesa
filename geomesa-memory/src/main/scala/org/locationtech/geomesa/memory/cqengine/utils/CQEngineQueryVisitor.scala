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
import org.locationtech.geomesa.memory.cqengine.query.{Intersects => CQIntersects}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.spatial._

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
      case c if classOf[Date             ].isAssignableFrom(c) => BuildDateBetweenQuery(prop)
    }
  }

  override def visit(filter: PropertyIsGreaterThan, data: scala.Any): cqquery.simple.GreaterThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTQuery(prop)
    }
  }

  override def visit(filter: PropertyIsGreaterThanOrEqualTo, data: scala.Any): cqquery.simple.GreaterThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntGTEQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongGTEQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatGTEQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleGTEQuery(prop)
    }
  }

  override def visit(filter: PropertyIsLessThan, data: scala.Any): cqquery.simple.LessThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTQuery(prop)
    }
  }

  override def visit(filter: PropertyIsLessThanOrEqualTo, data: scala.Any): cqquery.simple.LessThan[SimpleFeature, _] = {
    val prop = getAttributeProperty(filter).get
    sft.getDescriptor(prop.name).getType.getBinding match {
      case c if classOf[java.lang.Integer].isAssignableFrom(c) => BuildIntLTEQuery(prop)
      case c if classOf[java.lang.Long   ].isAssignableFrom(c) => BuildLongLTEQuery(prop)
      case c if classOf[java.lang.Float  ].isAssignableFrom(c) => BuildFloatLTEQuery(prop)
      case c if classOf[java.lang.Double ].isAssignableFrom(c) => BuildDoubleLTEQuery(prop)
    }
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
