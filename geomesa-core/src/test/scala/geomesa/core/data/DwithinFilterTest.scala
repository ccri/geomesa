package geomesa.core.data

import com.vividsolutions.jts.geom._
import geomesa.core.index.Constants
import geomesa.utils.geotools.GeometryUtils
import geomesa.utils.text.WKTUtils
import org.geotools.data.{DataStoreFinder, DataUtilities}
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class DwithinFilterTest extends Specification {

  var id = 0

  def createStore: AccumuloDataStore = {
    // need to add a unique ID, otherwise create schema will throw an exception
    id = id + 1

    // the specific parameter values should not matter, as we
    // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user" -> "myuser",
      "password" -> "mypassword",
      "tableName" -> ("DwithinFilterTest" + id),
      "useMock" -> "true"
    )).asInstanceOf[AccumuloDataStore]
  }

  val ff = CommonFactoryFinder.getFilterFactory2

  val sftName = "DwithinFilterTest"
  val dtg = "dtg"
  val schema = s"*geom:Geometry:srid=4326,$dtg:Date"
  val sft = DataUtilities.createType(sftName, schema)
  sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtg

  val ds = createStore

  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  val inputFeatures = new DefaultFeatureCollection(sftName, sft)
  for(lat <- (40 until 50);
      lon <- (40 until 50)
  ) {
    val sf = SimpleFeatureBuilder.build(sft, List(), s"${lon}.${lat}")
    sf.setDefaultGeometry(WKTUtils.read(s"POINT(${lon} ${lat})"))
    sf.setAttribute(dtg, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
    sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
    inputFeatures.add(sf)
  }

  fs.addFeatures(inputFeatures)

  val geomFactory = JTSFactoryFinder.getGeometryFactory

  "DwithinQueries" should {

    "handle simple Point geometries" in {
      val startPoint = geomFactory.createPoint(new Coordinate(45.0, 48.0))

      // specific point should match...lets make it within 1/10 mm
      val results = fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(startPoint), .0001, "meters"))
      results.size should equalTo(1)

      val features = results.features
      val f = features.next()
      f.getID mustEqual "45.48"
      features.hasNext must beFalse
    }

    "query points within acceptable error" in {
      val startPoint = geomFactory.createPoint(new Coordinate(45.0, 48.0))

      // point 100m away in all directions
      val far = GeometryUtils.farthestPoint(startPoint, 100.0)
      val results = fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(far), 100.0001, "meters"))
      results.size should equalTo(1)

      val features = results.features
      val f = features.next()
      f.getID mustEqual "45.48"
      features.hasNext must beFalse

      // find it farther away
      fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(far), 110, "meters")).size should equalTo(1)

      // shouldn't find this at 99.9999 meters
      fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(far), 99.99999, "meters")).size should equalTo(0)

      // shouldn't find this at 99.9999 meters
      fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(far), 98, "meters")).size should equalTo(0)
    }

    "handle simple LineString geometries" in {
      val line1 = WKTUtils.read("LINESTRING(40.8 40.8, 41.2 41.2)")

      val results = fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(line1), 0.0001, "meters"))
      results.size should equalTo(1)

      val features = results.features
      val f = features.next()
      f.getID mustEqual "41.41"
      features.hasNext must beFalse

    }

    "properly query Polygon geometries" in {
      val poly = WKTUtils.read("POLYGON ((47.99999999986587 47.99999999991005, 48.00000000013412 47.99999999991005, 48.00000000013412 48.000000000089955, 47.99999999986587 48.000000000089955, 47.99999999986587 47.99999999991005))").asInstanceOf[Polygon]

      val results = fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(poly), 0.0001, "meters"))
      results.size should equalTo(1)

      val features = results.features
      val f = features.next()
      f.getID mustEqual "48.48"
      features.hasNext must beFalse
    }

    "throw an exception on MultiPoint geometries" in {
      val multipoint = WKTUtils.read("MULTIPOINT((48 48), (49 49))").asInstanceOf[MultiPoint]

      val results = fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(multipoint), 10, "meters"))
      results.features must throwA[IllegalArgumentException]
    }

    "throw an exception on MultiLineString geometries" in {
      val multiLineString = WKTUtils.read("MULTILINESTRING ((40 40, 41 41, 42 42), (45 45, 45 46, 45 48))").asInstanceOf[MultiLineString]

      val results = fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(multiLineString), 10, "meters"))
      results.features must throwA[IllegalArgumentException]
    }

    "throw an exception on MultiPolygon geometries" in {
      val multiPoly = WKTUtils.read("MULTIPOLYGON (((39 39, 41 39, 41 41, 39 41, 39 39)),((44 44, 49 44, 49 49, 44 49, 44 44)))").asInstanceOf[MultiPolygon]

      val results = fs.getFeatures(ff.dwithin(ff.property("geom"), ff.literal(multiPoly), 10, "meters"))
      results.features must throwA[IllegalArgumentException]
    }
  }


}
