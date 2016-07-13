package org.locationtech.geomesa.accumulo.filter

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.specs2.mutable.Specification

class PolygonFilterTest extends Specification with TestWithDataStore with LazyLogging {
  override def spec: String = s"a:String,b:Integer,c:Double,geom:MultiPolygon:srid=4326,dtg:Date"

}
