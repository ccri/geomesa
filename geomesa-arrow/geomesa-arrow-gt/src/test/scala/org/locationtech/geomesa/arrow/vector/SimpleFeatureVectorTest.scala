/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.arrow.vector

import com.google.common.collect.ImmutableBiMap
import org.apache.arrow.memory.RootAllocator
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureVectorTest extends Specification {

  import scala.collection.JavaConversions._

  "SimpleFeatureVector" should {
    "set and get values" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features = (0 until 10).map { i =>
        ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
      }
      implicit val allocator = new RootAllocator(Long.MaxValue)
      try {
        val vector = SimpleFeatureVector.create(sft, Map.empty)
        try {
          features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
          vector.writer.setValueCount(features.length)
          vector.reader.getValueCount mustEqual features.length
          forall(0 until 10) { i =>
            vector.reader.get(i) mustEqual features(i)
          }
        } finally {
          vector.close()
        }
      } finally {
        allocator.close()
      }
    }
    "set and get dictionary encoded values" >> {
      val sft = SimpleFeatureTypes.createType("test", "name:String,dtg:Date,*geom:Point:srid=4326")
      val features = (0 until 10).map { i =>
        ScalaSimpleFeature.create(sft, s"0$i", s"name0${i % 2}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
      }
      implicit val allocator = new RootAllocator(Long.MaxValue)
      try {
        val dictionaryMap = ImmutableBiMap.builder[AnyRef, Int].putAll(Map("name00" -> 0, "name01" -> 1)).build()
        val dictionary = new ArrowDictionary(dictionaryMap)
        val vector = SimpleFeatureVector.create(sft, Map("name" -> dictionary))
        try {
          features.zipWithIndex.foreach { case (f, i) => vector.writer.set(i, f) }
          vector.writer.setValueCount(features.length)
          vector.reader.getValueCount mustEqual features.length
          forall(0 until 10) { i =>
            vector.reader.get(i) mustEqual features(i)
          }
        } finally {
          vector.close()
        }
      } finally {
        allocator.close()
      }
    }
  }
}
