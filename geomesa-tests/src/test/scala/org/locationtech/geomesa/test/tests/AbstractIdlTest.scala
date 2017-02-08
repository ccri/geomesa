package org.locationtech.geomesa.test.tests

import org.geotools.data.Query
import org.geotools.factory.CommonFactoryFinder
import org.geotools.referencing.CRS
import org.locationtech.geomesa.test.api.WithDataStore
//import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.specs2.mutable.Specification
import org.locationtech.geomesa.utils.geotools.Conversions._

trait AbstractIdlTest extends Specification with WithDataStore { //with TestWithDataStore {

    sequential

    val spec = "dtg:Date,*geom:Point:srid=4326"

    addFeatures((-180 to 180).map { lon =>
      val sf = new ScalaSimpleFeature(lon.toString, sft)
      sf.setAttribute(0, "2015-01-01T00:00:00.000Z")
      sf.setAttribute(1, s"POINT($lon ${lon / 10})")
      sf
    })

    val ff = CommonFactoryFinder.getFilterFactory2
    val srs = CRS.toSRS(org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326)

    "AccumuloDataStore" should {

      "handle IDL correctly" in {
        "default layer preview, bigger than earth, multiple IDL-wrapping geoserver BBOX" in {
          val filter = ff.bbox("geom", -230, -110, 230, 110, srs)
          val query = new Query(sft.getTypeName, filter)
          val results = fs.getFeatures(query).features.map(_.getID)
          results must haveLength(361)
        }

        "greater than 180 lon diff non-IDL-wrapping geoserver BBOX" in {
          val filter = ff.bbox("geom", -100, 1.1, 100, 4.1, srs)
          val query = new Query(sft.getTypeName, filter)
          val results = fs.getFeatures(query).features.map(_.getID)
          results must haveLength(30)
        }

        "small IDL-wrapping geoserver BBOXes" in {
          val spatial1 = ff.bbox("geom", -181.1, -30, -175.1, 30, srs)
          val spatial2 = ff.bbox("geom", 175.1, -30, 181.1, 30, srs)
          val filter = ff.or(spatial1, spatial2)
          val query = new Query(sft.getTypeName, filter)
          val results = fs.getFeatures(query).features.map(_.getID)
          results must haveLength(10)
        }

        "large IDL-wrapping geoserver BBOXes" in {
          val spatial1 = ff.bbox("geom", -181.1, -30, 40.1, 30, srs)
          val spatial2 = ff.bbox("geom", 175.1, -30, 181.1, 30, srs)
          val filter = ff.or(spatial1, spatial2)
          val query = new Query(sft.getTypeName, filter)
          val results = fs.getFeatures(query).features.map(_.getID)
          results must haveLength(226)
        }
      }
    }
  }