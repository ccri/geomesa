/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.filter.function

import com.vividsolutions.jts.geom.Point
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.feature.{AttributeTypeBuilder, NameImpl}
import org.geotools.filter.spatial.BBOXImpl
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class JsonPathTest extends Specification {
  val ff = CommonFactoryFinder.getFilterFactory2

  "JsonPath" should {
    "evaluate" >> {
      val filter = ECQL.toFilter("jsonpath(data, foo) = 'bar'")

      val sft = SimpleFeatureTypes.createType("FastPropertyTest", "name:String,*geom:Point:srid=4326,data:String")
      val sf = new ScalaSimpleFeature("id", sft)
      sf.setAttributes(Array[AnyRef]("myname", "POINT(45 45)", """{ "foo" : "bar" }"""))

      filter.evaluate(sf) mustEqual true
      success
    }
  }
}