package org.locationtech.geomesa.utils.time

import org.joda.time.{Interval, DateTimeZone, DateTime}
import org.junit.runner.RunWith
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.utils.time.Time._

import scala.collection.immutable.HashMap

@RunWith(classOf[JUnitRunner])
class RichIntervalTest extends Specification {

  "RichIntervals" should {
    val dt1 = new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC)
    val dt2 = new DateTime("2012-02-02T02:00:00", DateTimeZone.UTC)
    val dt3 = new DateTime("2012-03-03T03:00:00", DateTimeZone.UTC)
    val dt4 = new DateTime("2012-04-04T04:00:00", DateTimeZone.UTC)
    val dt5 = new DateTime("2012-05-05T05:00:00", DateTimeZone.UTC)


    val int1 = new Interval(dt1, dt2)
    val int2 = new Interval(dt2, dt3)
    val int3 = new Interval(dt1, dt3)

    "support unions" >> {
      val union = int1.getSafeUnion(int2)
      union.equals(int3)
    }


  }


}