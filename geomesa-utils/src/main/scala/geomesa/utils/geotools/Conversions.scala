/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.utils.geotools

import com.vividsolutions.jts.geom.{GeometryFactory, Point, Geometry, Coordinate}
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.factory.Hints
import org.geotools.geometry.DirectPosition2D
import org.geotools.temporal.`object`.{DefaultPosition, DefaultInstant, DefaultPeriod}
import org.geotools.util.{Converter, ConverterFactory}
import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.opengis.feature.simple.SimpleFeature
import org.opengis.temporal.Instant
import org.geotools.referencing.GeodeticCalculator
import org.geotools.feature.simple.SimpleFeatureImpl

object Conversions {

  class RichSimpleFeatureIterator(iter: SimpleFeatureIterator) extends SimpleFeatureIterator
      with Iterator[SimpleFeature] {
    private[this] var open = true

    def isClosed = !open

    def hasNext = {
      if (isClosed) false
      if(iter.hasNext) true else{close(); false}
    }
    def next() = iter.next
    def close() { if(!isClosed) {iter.close(); open = false} }
  }

  implicit def toRichSimpleFeatureIterator(iter: SimpleFeatureIterator) = new RichSimpleFeatureIterator(iter)
  implicit def opengisInstantToJodaInstant(instant: Instant): org.joda.time.Instant = new DateTime(instant.getPosition.getDate).toInstant
  implicit def jodaInstantToOpengisInstant(instant: org.joda.time.Instant): org.opengis.temporal.Instant = new DefaultInstant(new DefaultPosition(instant.toDate))
  implicit def jodaIntervalToOpengisPeriod(interval: org.joda.time.Interval): org.opengis.temporal.Period =
    new DefaultPeriod(interval.getStart.toInstant, interval.getEnd.toInstant)


  implicit class RichCoord(val c: Coordinate) extends AnyVal {
    def toPoint2D = new DirectPosition2D(c.x, c.y)
  }

  class RichGeometry(val geom: Geometry) {
    val calc = new GeodeticCalculator()
    val geoFac = new GeometryFactory

    def bufferMeters(meters: Double): Geometry = geom.buffer(metersToDegrees(meters, geom.getCentroid))

    // TODO cache the metersToDegrees
    def metersToDegrees(meters: Double, point: Point) = {
      calc.setStartingGeographicPoint(point.getX, point.getY)
      calc.setDirection(0, meters)
      val dest2D = calc.getDestinationGeographicPoint
      val destPoint = geoFac.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
      point.distance(destPoint)
    }
  }

  implicit def toRichGeometry(geom: Geometry) = new RichGeometry(geom)

//  class RichSimpleFeature(val sf: SimpleFeature) extends SimpleFeatureImpl(sf.getAttributes, sf.getFeatureType, sf.getIdentifier) {
//
//    def bufferMeters(meters: Double) = {
//      val geom = getDefaultGeometry.asInstanceOf[Geometry]
//      geom.bufferMeters(meters)
//    }
//
//
//  }
//
//  implicit def toRichSimpleFeature(sf: SimpleFeature) = new RichSimpleFeature(sf)
}

class JodaConverterFactory extends ConverterFactory {
  private val df = ISODateTimeFormat.dateTime()
  def createConverter(source: Class[_], target: Class[_], hints: Hints) =
    if(classOf[java.util.Date].isAssignableFrom(source) && classOf[String].isAssignableFrom(target)) {
      // Date => String
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          df.print(new DateTime(source.asInstanceOf[java.util.Date])).asInstanceOf[T]
      }
    } else if(classOf[java.util.Date].isAssignableFrom(target) && classOf[String].isAssignableFrom(source)) {
      // String => Date
      new Converter {
        def convert[T](source: scala.Any, target: Class[T]): T =
          df.parseDateTime(source.asInstanceOf[String]).toDate.asInstanceOf[T]
      }
    } else null.asInstanceOf[Converter]
}
