package org.locationtech.geomesa.utils.geotools

import java.util.concurrent.atomic.AtomicBoolean

import org.geotools.data.store.ContentFeatureSource
import org.geotools.data.{DelegatingFeatureReader, FeatureReader, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class MonitoringFeatureReader(query: Query, delegate: FeatureReader[SimpleFeatureType, SimpleFeature])
  extends DelegatingFeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  protected var counter = 0L

  private val closed = new AtomicBoolean(false)

  implicit val timings = new TimingsImpl

  override def getDelegate: FeatureReader[SimpleFeatureType, SimpleFeature] = delegate
  override def getFeatureType: SimpleFeatureType = delegate.getFeatureType

  override def next(): SimpleFeature = {
    counter += 1
    profile(delegate.next(), "next")
  }
  override def hasNext: Boolean = profile(delegate.hasNext, "hasNext")

  override def close() = if (!closed.getAndSet(true)) {
    closeOnce()
  }

  protected def closeOnce(): Unit = {

    val stat = GeneralUsageStat(getFeatureType.getTypeName,
      System.currentTimeMillis(),
      ECQL.toCQL(query.getFilter),
      timings.time("next") + timings.time("hasNext"),
      counter
    )
    delegate.close()
    Monitoring.log(stat)
  }
}

trait MonitoringFeatureSourceSupport extends ContentFeatureSourceSupport {
  self: ContentFeatureSource =>      // JNH: Do we need the self type?

  override def addSupport(query: Query, reader: FR): FR = {
    val parentReader = super.addSupport(query, reader)
    new MonitoringFeatureReader(query, parentReader)
  }

}

case class GeneralUsageStat(typeName: String, date: Long, filter: String, totalTime: Long, count: Long) extends UsageStat
