package org.locationtech.geomesa.utils.geotools

import java.util.Date

import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.CommonFactoryFinder
import org.joda.time.Interval
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.time.Time._
import org.opengis.feature.{Feature, FeatureVisitor}

class MinMaxTimeVisitor(dtg: String) extends FeatureVisitor {

  def computeMinMaxTime(source: SimpleFeatureSource, query: Query) {
    query.setPropertyNames(Array(dtg))
    val feats = source.getFeatures(query).features()

    feats.foreach { f => timeBounds = timeBounds.expandByDate(f.getAttribute(dtg).asInstanceOf[Date]) }
  }

  val factory = CommonFactoryFinder.getFilterFactory(null)
  val expr = factory.property(dtg)

  var timeBounds: Interval = null

  // TODO?: Convert to CalcResults/etc?
  def getBounds = timeBounds

  override def visit(p1: Feature): Unit = {
    val date = expr.evaluate(p1).asInstanceOf[Date]
    //println(s"Feature $p1 has date $date")
    if (date != null) {
      timeBounds = timeBounds.expandByDate(date)
    }
  }
}
