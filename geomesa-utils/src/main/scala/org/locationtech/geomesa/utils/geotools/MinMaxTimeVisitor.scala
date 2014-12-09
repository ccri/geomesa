package org.locationtech.geomesa.utils.geotools

import java.util.Date

import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.factory.CommonFactoryFinder
import org.opengis.feature.{Feature, FeatureVisitor}

import Conversions._

class MinMaxTimeVisitor(dtg: String) extends FeatureVisitor {

  def getMinMaxTime(source: SimpleFeatureSource, query: Query): Seq[Date] = {
    query.setPropertyNames(Array(dtg))
    val feats = source.getFeatures(query).features()

    feats.foldLeft(Seq(new Date(0), new Date())) {
      case (dates, feature) =>
        val featureDate = feature.getAttribute(dtg).asInstanceOf[Date]
        val allThree = (dates :+ featureDate).sorted
        Seq(allThree.head, allThree.last)
    }
  }

  def setValue(value: Seq[Date]) = { timeBounds = value }

  val factory = CommonFactoryFinder.getFilterFactory(null)
  val expr = factory.property(dtg)

  var timeBounds: Seq[Date] = null

  // TODO: Convert to CalcResults/etc
  def getBounds = timeBounds

  override def visit(p1: Feature): Unit = {

    val date = expr.evaluate(p1).asInstanceOf[Date]
    //println(s"Feature $p1 has date $date")
    if (date != null) {
      timeBounds = updateBounds(date)
    }
  }

  def updateBounds(date: Date) = {
    if(timeBounds == null) Seq(date, date)
    else if (date.compareTo(timeBounds(0)) < 0) Seq(date, timeBounds(1))
    else if (date.compareTo(timeBounds(1)) > 0) Seq(timeBounds(0), date)
    else timeBounds
  }
}
