/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.util.{UUID, Date}

import com.amazonaws.services.dynamodbv2.document.{KeyAttribute, Item, Table}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.simple.SimpleFeatureWriter
import org.joda.time.{Seconds, Weeks, DateTime}
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

trait DynamoDBFeatureWriter extends SimpleFeatureWriter {
  private val SFC3D = new Z3SFC
  private var curFeature: SimpleFeature = null
  private val dtgIndex =
    sft.getAttributeDescriptors
      .zipWithIndex
      .find { case (ad, idx) => classOf[java.util.Date].equals(ad.getType.getBinding) }
      .map  { case (_, idx)  => idx }
      .getOrElse(throw new RuntimeException("No date attribute"))

  private val encoder = new KryoFeatureSerializer(sft)

  def sft: SimpleFeatureType
  def table: Table

  override def next(): SimpleFeature = {
    curFeature = new ScalaSimpleFeature(UUID.randomUUID().toString, sft)
    curFeature
  }

  override def remove(): Unit = ???

  override def hasNext: Boolean = true

  val EPOCH = new DateTime(0)

  def epochWeeks(dtg: DateTime): Weeks = Weeks.weeksBetween(EPOCH, new DateTime(dtg))

  def secondsInCurrentWeek(dtg: DateTime, weeks: Weeks): Int =
    Seconds.secondsBetween(EPOCH, dtg).getSeconds - weeks.toStandardSeconds.getSeconds

  override def write(): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions._

    // write
    val geom = curFeature.point
    val x = geom.getX
    val y = geom.getY
    val dtg = new DateTime(curFeature.getAttribute(dtgIndex).asInstanceOf[Date])
    val weeks = epochWeeks(dtg)

    val secondsInWeek = secondsInCurrentWeek(dtg, weeks)
    val z3 = SFC3D.index(x, y, secondsInWeek)

    val id = curFeature.getID
    val item = new Item().withKeyComponents(new KeyAttribute("id", id), new KeyAttribute("z3", z3.z))

    curFeature.getAttributes.zip(sft.getAttributeDescriptors).foreach { case (attr, desc) =>
      import java.{lang => jl}
      desc.getType.getBinding match {
        case c if c.equals(classOf[String]) =>
          item.withString(desc.getLocalName, attr.asInstanceOf[String])

        case c if c.equals(classOf[jl.Integer]) =>
          item.withInt(desc.getLocalName, attr.asInstanceOf[jl.Integer])

        case c if c.equals(classOf[jl.Double]) =>
          item.withDouble(desc.getLocalName, attr.asInstanceOf[jl.Double])

        case c if c.equals(classOf[java.util.Date]) =>
          item.withLong(desc.getLocalName, attr.asInstanceOf[java.util.Date].getTime)

        case c if classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(c) =>
          item.withBinary(desc.getLocalName, WKBUtils.write(attr.asInstanceOf[Geometry]))
      }
    }

    item.withBinary("ser", encoder.serialize(curFeature))
    table.putItem(item)
    curFeature = null
  }

  override def getFeatureType: SimpleFeatureType = sft

  override def close(): Unit = {}

}

class DynamoDBAppendingFeatureWriter(val sft: SimpleFeatureType, val table: Table) extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false
}

class DynamoDBUpdatingFeatureWriter(val sft: SimpleFeatureType, val table: Table) extends DynamoDBFeatureWriter {
  override def hasNext: Boolean = false
}