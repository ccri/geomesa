/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geoserver

import java.util.Collections

import org.geotools.data.Query
import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.junit.runner.RunWith
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.utils.geotools.{CRS_EPSG_4326, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, FilterFactory2, IncludeFilter}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class ViewParamsTest extends Specification {

  import org.locationtech.geomesa.index.conf.QueryHints._

  val ff: FilterFactory2 = CommonFactoryFinder.getFilterFactory2

  val wkt = "POLYGON((-80 35,-70 35,-70 40,-80 40,-80 35))"


  "ViewParams" should {
    "handle all types of query hints" in {
      def testHint(hint: Hints.Key, name: String, param: String, expected: Any): MatchResult[Any] = {
        val query = new Query()
        query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, Collections.singletonMap(name, param))
        ViewParams.setHints(null, query)
        query.getHints.get(hint) mustEqual expected
      }

      testHint(QUERY_INDEX, "QUERY_INDEX", "index-test", "index-test")
      testHint(BIN_TRACK, "BIN_TRACK", "track", "track")
      testHint(COST_EVALUATION, "COST_EVALUATION", "stats", CostEvaluation.Stats)
      testHint(DENSITY_BBOX, "DENSITY_BBOX", "[-120.0, -45, 10, -35.01]", new ReferencedEnvelope(-120d, 10d, -45d, -35.01d, CRS_EPSG_4326))
      testHint(ENCODE_STATS, "ENCODE_STATS", "true", true)
      testHint(ENCODE_STATS, "ENCODE_STATS", "false", false)
      testHint(DENSITY_WIDTH, "DENSITY_WIDTH", "640", 640)
      testHint(SAMPLING, "SAMPLING", "0.4", 0.4f)
    }

    "allow for updating a query with an equality filter" in {
      val sft: SimpleFeatureType = SimpleFeatureTypes.createType("testType", "name:String,*geom:Point,dtg:Date,attr1:String,attr2:Long")

      val query = new Query()
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, Collections.singletonMap("name", "Bill"))
      ViewParams.setHints(sft, query)
      query.getFilter.isInstanceOf[IncludeFilter] mustEqual false
      val filter = ff.and(ff.equal(ff.property("name"), ff.literal("Bill")), Filter.INCLUDE)
      query.getFilter mustEqual filter
    }

    "allow for updating a query with a geometry filter" in {
      val sft: SimpleFeatureType = SimpleFeatureTypes.createType("testType", "name:String,*geom:Point,dtg:Date,attr1:String,attr2:Long")

      val query = new Query()
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, Collections.singletonMap("geom", wkt))
      ViewParams.setHints(sft, query)
      query.getFilter.isInstanceOf[IncludeFilter] mustEqual false
      val filter = ff.and(ff.intersects(ff.property("geom"), ff.literal(WKTUtils.read(wkt))), Filter.INCLUDE)
      query.getFilter mustEqual filter
    }

    "allow for updating a query with multiple filters" in {
      val sft: SimpleFeatureType = SimpleFeatureTypes.createType("testType", "name:String,*geom:Point,dtg:Date,attr1:String,attr2:Long")

      val query = new Query()
      query.getHints.put(Hints.VIRTUAL_TABLE_PARAMETERS, Map("geom" -> wkt, "name" -> "Bill").asJava)
      ViewParams.setHints(sft, query)
      query.getFilter.isInstanceOf[IncludeFilter] mustEqual false
      FilterHelper.flatten(query.getFilter).asInstanceOf[And].getChildren.size mustEqual 3
    }
  }
}
