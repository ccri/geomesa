package geomesa.core.data

import collection.JavaConversions._
import com.vividsolutions.jts.geom.{Polygon, Coordinate}
import geomesa.core.index.Constants
import geomesa.utils.text.WKTUtils
import org.geotools.data.DataUtilities
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.geometry.jts.{JTS, ReferencedEnvelope, JTSFactoryFinder}
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTimeZone, DateTime, Interval}
import org.junit.runner.RunWith
import org.opengis.filter.Filter
import org.opengis.filter.spatial.DWithin
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterToAccumulo2Test extends Specification {

  val WGS84       = DefaultGeographicCRS.WGS84
  val ff          = CommonFactoryFinder.getFilterFactory2
  val geomFactory = JTSFactoryFinder.getGeometryFactory
  val sft         = DataUtilities.createType("test", "id:Integer,prop:String,dtg:Date,otherGeom:Geometry:srid=4326,*geom:Point:srid=4326")
  sft.getUserData.put(Constants.SF_PROPERTY_START_TIME, "dtg")

  "BBOX queries" should {
    "set the spatial predicate and simplify the query" in {
      val q = ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(q)
      result mustEqual Filter.INCLUDE
    }

    "set the spatial predicate and remove from the subsequent query" in {
      val q =
        ff.and(
          ff.like(ff.property("prop"), "foo"),
          ff.bbox("geom", -80.0, 30, -70, 40, CRS.toSRS(WGS84))
        )
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(q)
      result mustEqual ff.like(ff.property("prop"), "foo")
    }
  }

  "DWithin queries" should {
    val targetPoint = geomFactory.createPoint(new Coordinate(-70, 30))

    "take in meters" in {
      val q =
        ff.dwithin(ff.property("geom"), ff.literal(targetPoint), 100.0, "meters")

      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(q)
      val expected = WKTUtils.read("POLYGON ((-69.99909789983381 30, -69.99911523343555 29.99982400898809, -69.99916656812019 29.999654781212065, -69.99924993112457 29.99949882000046, -69.99936211885517 29.999362118855178, -69.99949882000047 29.99924993112456, -69.99965478121207 29.99916656812018, -69.99982400898809 29.999115233435553, -70 29.99909789983381, -70.00017599101191 29.999115233435553, -70.00034521878793 29.99916656812018, -70.00050117999953 29.99924993112456, -70.00063788114483 29.999362118855178, -70.00075006887543 29.99949882000046, -70.00083343187981 29.999654781212065, -70.00088476656445 29.99982400898809, -70.00090210016619 30, -70.00088476656445 30.00017599101191, -70.00083343187981 30.000345218787935, -70.00075006887543 30.00050117999954, -70.00063788114483 30.000637881144822, -70.00050117999953 30.00075006887544, -70.00034521878793 30.00083343187982, -70.00017599101191 30.000884766564447, -70 30.00090210016619, -69.99982400898809 30.000884766564447, -69.99965478121207 30.00083343187982, -69.99949882000047 30.00075006887544, -69.99936211885517 30.000637881144822, -69.99924993112457 30.00050117999954, -69.99916656812019 30.000345218787935, -69.99911523343555 30.00017599101191, -69.99909789983381 30))").asInstanceOf[Polygon]
      val resultEnv = f2a.spatialPredicate
      resultEnv.equalsNorm(expected) must beTrue
      result.asInstanceOf[DWithin].getDistance mustEqual 0.0010364167811696486
    }
  }

  "Within queries" should {
    "set the spatial predicate and simplify the query if rectangular" in {
      val rectWithin =
        ff.within(
          ff.property("geom"),
          ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(rectWithin)
      result mustEqual Filter.INCLUDE
    }

    "set the spatial predicate and keep the geom query if not rectangular" in {
      val rectWithin =
        ff.within(
          ff.property("geom"),
          ff.literal(WKTUtils.read("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))")))
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(rectWithin)
      result mustNotEqual Filter.INCLUDE
    }
  }

  "Temporal queries" should {
    "set the temporal predicate and simplify the query" in {
      val pred = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "with spatial queries should simplify the query" in {
      val temporal = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val spatial = ff.within(ff.property("geom"), ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val pred = ff.and(temporal, spatial)
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result mustEqual Filter.INCLUDE
    }

    "with spatial queries and property queries should simplify the query" in {
      val temporal = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val spatial = ff.within(ff.property("geom"), ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val prop = ff.like(ff.property("prop"), "FOO%")
      val pred = ff.and(List(temporal, spatial, prop))
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result.toString mustEqual prop.toString
    }
  }

  "Logic queries" should {
    "keep property queries" in {
      val temporal = ECQL.toFilter("dtg DURING 2011-01-01T00:00:00Z/2011-02-01T00:00:00Z")
      val spatial = ff.within(ff.property("geom"), ff.literal(WKTUtils.read("POLYGON((-80 30,-70 30,-70 40,-80 40,-80 30))")))
      val prop1 = ff.like(ff.property("prop"), "FOO%")
      val prop2 = ff.like(ff.property("prop"), "BAR%")
      val prop = ff.and(prop1, prop2)
      val pred = ff.and(List(temporal, spatial, prop))
      val f2a = new FilterToAccumulo2(sft)
      val result = f2a.visit(pred)
      val interval = new Interval(
        new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC),
        new DateTime("2011-02-01T00:00:00Z", DateTimeZone.UTC))
      f2a.temporalPredicate mustEqual interval
      result.toString mustEqual prop.toString
    }
  }

}
