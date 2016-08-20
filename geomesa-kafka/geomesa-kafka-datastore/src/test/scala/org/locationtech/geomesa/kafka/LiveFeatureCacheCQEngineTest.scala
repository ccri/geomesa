/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import com.google.common.base.Ticker
import com.vividsolutions.jts.geom.Envelope
import org.joda.time.Instant
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class LiveFeatureCacheCQEngineTest extends Specification with Mockito with SimpleFeatureMatchers {

  import KafkaConsumerTestData._

  implicit val ticker = Ticker.systemTicker()
  val wholeWorld = new Envelope(-180, 180, -90, 90)

  "LiveFeatureCache" should {

    "handle a CreateOrUpdate message" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      lfc.size() mustEqual 1
      lfc.getFeatureById("track0") must equalFeatureHolder(track0v0)

      // TODO:  Add back spatial query.
    }

    "handle two CreateOrUpdate messages" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))

      lfc.size() mustEqual 2
      lfc.getFeatureById("track1") must equalFeatureHolder(track1v0)

      // TODO:  Add back spatial query.
    }

    "use the most recent version of a feature" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))

      lfc.size() mustEqual 2
      lfc.getFeatureById("track0") must equalFeatureHolder(track0v1)

      // TODO:  Add back spatial query.
    }

    "handle a Delete message" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))
      lfc.removeFeature(Delete(new Instant(4000), "track0"))

      lfc.size() mustEqual 1
      lfc.getFeatureById("track0") must beNull

      // TODO:  Add back spatial query.
    }

    "handle a Clear message" >> {
      val lfc = new LiveFeatureCacheCQEngine(sft, None)

      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(2000), track1v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(3000), track0v1))
      lfc.removeFeature(Delete(new Instant(4000), "track0"))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5000), track2v0))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5005), track1v2))
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(5010), track3v0))

      lfc.clear()

      lfc.size() mustEqual 0

      // TODO:  Add back spatial query.
    }
  }


  "LiveFeatureCache with expiry" should {

    "handle a CreateOrUpdate message" >> {
      implicit val ticker = new MockTicker
      ticker.tic = 1000000L // ns

      val lfc = new LiveFeatureCacheCQEngine(sft, Some(5L)) // ms
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      ticker.tic = 2000000L // ns

      lfc.size() mustEqual 1
      lfc.getFeatureById("track0") must equalFeatureHolder(track0v0)

      // TODO:  Add back spatial query.
    }

    "expire message correctly" >> {
      //pending("expiration not implemented yet")

      implicit val ticker = new MockTicker
      ticker.tic = 1000000L // ns

      val lfc = new LiveFeatureCacheCQEngine(sft, Some(5L)) // ms
      lfc.createOrUpdateFeature(CreateOrUpdate(new Instant(1000), track0v0))

      ticker.tic = 7000000L
      lfc.cleanUp()

      lfc.size() mustEqual 0
      lfc.getFeatureById("track0") must beNull

      // TODO:  Add back spatial query.
    }
  }
}

/*class MockTicker extends Ticker {

  var tic: Long = 0L

  def read(): Long = tic
}
*/

