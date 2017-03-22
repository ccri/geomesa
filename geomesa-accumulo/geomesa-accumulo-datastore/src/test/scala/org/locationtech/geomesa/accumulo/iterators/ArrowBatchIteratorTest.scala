/*******************************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ******************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.arrow.memory.RootAllocator
import org.geotools.data.{Query, Transaction}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.arrow.io.SimpleFeatureArrowFileReader
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.io.WithClose
import org.opengis.filter.Filter
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ArrowBatchIteratorTest extends TestWithDataStore {

  override val spec = "name:String,dtg:Date,*geom:Point:srid=4326"

  implicit val allocator = new RootAllocator(Long.MaxValue)

  val features = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name${i % 2}", s"2017-02-03T00:0$i:00.000Z", s"POINT(40 6$i)")
  }

  addFeatures(features)

  "ArrowBatchIterator" should {
    "return arrow encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))
      val in = new ByteArrayInputStream(out.toByteArray)
      implicit val allocator = new RootAllocator(Long.MaxValue)
      WithClose(new SimpleFeatureArrowFileReader(in)) { reader =>
        reader.read().toSeq must containTheSameElementsAs(features)
      }
    }
    "return arrow dictionary encoded data" in {
      val query = new Query(sft.getTypeName, Filter.INCLUDE)
      query.getHints.put(QueryHints.ARROW_ENCODE, true)
      query.getHints.put(QueryHints.ARROW_DICTIONARY, "name")
      val results = SelfClosingIterator(ds.getFeatureReader(query, Transaction.AUTO_COMMIT))
      val out = new ByteArrayOutputStream
      results.foreach(sf => out.write(sf.getAttribute(0).asInstanceOf[Array[Byte]]))

      def in = new ByteArrayInputStream(out.toByteArray)

      WithClose(new SimpleFeatureArrowFileReader(in, decodeDictionaries = true)) { reader =>
        reader.read().toSeq must containTheSameElementsAs(features)
      }
      WithClose(new SimpleFeatureArrowFileReader(in, decodeDictionaries = false)) { reader =>
        val encoded = features.map { f =>
          val attributes = f.getAttributes.toArray
          attributes(0) = if (f.getAttribute(0) == "name0") Int.box(1) else Int.box(0)
          // set the values directly so the dictionary doesn't get converted to a string
          new ScalaSimpleFeature(f.getID, sft, attributes)
        }
        reader.read().toSeq must containTheSameElementsAs(encoded)
      }
    }
  }

  step {
    allocator.close()
  }
}
