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

  var timeBounds = Seq(new Date(0), new Date())

  // TODO: Convert to CalcResults/etc
  def getBounds = timeBounds

  override def visit(p1: Feature): Unit = {

    val date = expr.evaluate(p1).asInstanceOf[Date]
    println(s"Feature $p1 has date $date")
    if (date != null) {
      updateBounds(date)
    }
  }

  def updateBounds(date: Date): Unit = {
    val allThree = (timeBounds :+ date).sorted
    timeBounds = Seq(allThree.head, allThree.last)
  }
}
