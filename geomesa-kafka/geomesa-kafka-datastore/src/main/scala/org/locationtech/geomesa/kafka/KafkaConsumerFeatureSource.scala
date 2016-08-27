/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import java.io.Serializable
import java.{util => ju}

import com.vividsolutions.jts.geom.Envelope
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.data.{FilteringFeatureReader, Query}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.NameImpl
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.kafka.KafkaDataStore.FeatureSourceFactory
import org.locationtech.geomesa.kafka.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.security.ContentFeatureSourceSecuritySupport
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.index.QuadTreeFeatureStore
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter._
import org.opengis.filter.spatial.{BBOX, Intersects, Within}

import scala.collection.JavaConversions._
import scala.collection.mutable

abstract class KafkaConsumerFeatureSource(entry: ContentEntry,
                                          sft: SimpleFeatureType,
                                          query: Query,
                                          monitor: Boolean)
  extends ContentFeatureSource(entry, query)
  with ContentFeatureSourceSecuritySupport
  with ContentFeatureSourceReTypingSupport
  with ContentFeatureSourceInfo {

  import org.locationtech.geomesa.utils.geotools._

  override def getBoundsInternal(query: Query) = KafkaConsumerFeatureSource.wholeWorldBounds

  override def buildFeatureType(): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.setNamespaceURI(getDataStore.getNamespaceURI)
    sft.getUserData.foreach { case (k, v) => builder.userData(k, v)}
    builder.buildFeatureType()
  }

  override def getCountInternal(query: Query): Int = getReaderInternal(query).getIterator.length

  override val canFilter: Boolean = true

  override def getReaderInternal(query: Query): FR = if (monitor) {
    new MonitoringFeatureReader("Kafka", query, addSupport(query, getReaderForFilter(query.getFilter)))
  } else {
    addSupport(query, getReaderForFilter(query.getFilter))
  }

  def getReaderForFilter(f: Filter): FR

  lazy val fqName = new NameImpl(getDataStore.getNamespaceURI, getSchema.getName.getLocalPart)

  override def getName: Name = fqName

}

object KafkaConsumerFeatureSource {
  lazy val wholeWorldBounds = ReferencedEnvelope.create(new Envelope(-180, 180, -90, 90), CRS_EPSG_4326)
}

case class FeatureHolder(sf: SimpleFeature, env: Envelope) {
  override def hashCode(): Int = sf.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case other: FeatureHolder => sf.equals(other.sf)
    case _ => false
  }
}

trait KafkaConsumerFeatureCache extends QuadTreeFeatureStore {

  def features: mutable.Map[String, FeatureHolder]
  private val ff = CommonFactoryFinder.getFilterFactory2

  // optimized for filter.include
  def size(f: Filter): Int = {
    if (f == Filter.INCLUDE) {
      features.size
    } else {
      getReaderForFilter(f).getIterator.length
    }
  }

  def size(): Int = {
    features.size
  }

  def getReaderForFilter(filter: Filter): FR =
    filter match {
      case f: IncludeFilter => include(f)
      case f: Id            => fid(f)
      case f: And           => and(f)
      case f: BBOX          => /* println(s"Running BBOX; $f"); */ bbox(f)
      case f: Intersects    => /* println(s"Running INTERSECTS; $f"); */ intersects(f)
      case f: Within        => /* println(s"Running Within; $f"); */ within(f)
      case f: Or            => or(f)
      case f                => unoptimized(f)
    }

  def include(i: IncludeFilter) = {
    //println("Running Filter.INCLUDE")
    new DFR(sft, new DFI(features.valuesIterator.map(_.sf)))
  }

  def fid(ids: Id): FR = {
    //println("Queried for IDs; using feature ID index")
    val iter = ids.getIDs.flatMap(id => features.get(id.toString).map(_.sf)).iterator
    new DFR(sft, new DFI(iter))
  }

  def and(a: And): FR = {
    //println(s"Running an AND query for filter: $a")
    val geometries = extractGeometries(a, sft.getGeometryDescriptor.getLocalName)
    if (geometries.isEmpty) {
      //println("Could not extract geometries; running unoptimized")
      unoptimized(a)
    } else {
      val set = geometries.toSet

      if (set.size == 1) {
        val envelope = set.head.getEnvelopeInternal
        new DFR(sft, new DFI(spatialIndex.query(envelope, a.evaluate)))
      } else if (set.size == 2 && !set.head.overlaps(set.tail.head)) {
        //println("Got to disjoint geometries!")

        val env1 = set.head.getEnvelopeInternal
        val env2 = set.tail.head.getEnvelopeInternal

        new DFR(sft, new DFI(
          spatialIndex.query(set.head.getEnvelopeInternal, a.evaluate) ++
          spatialIndex.query(set.tail.head.getEnvelopeInternal, a.evaluate)))
      } else {
        //println("Handling the ")
        val envelope = geometries.head.getEnvelopeInternal
        geometries.tail.foreach(g => envelope.expandToInclude(g.getEnvelopeInternal))
        geometries

        //println(s"Calling out to the spatial index with envelope $envelope")
        new DFR(sft, new DFI(spatialIndex.query(envelope, a.evaluate)))
      }
    }
  }

  def or(o: Or): FR = {
    //println(s"Running an or query for $o")
    val readers = o.getChildren.map { f => /* println(s"Running filter separately for $f") */; getReaderForFilter(f)}.map(_.getIterator)
    val composed = readers.foldLeft(Iterator[SimpleFeature]())(_ ++ _)
    new DFR(sft, new DFI(composed))
  }

  def unoptimized(f: Filter): FR = {
    //println(s"Running filter unoptimized: $f")
    new FilteringFeatureReader[SimpleFeatureType, SimpleFeature](include(Filter.INCLUDE), f)
  }
}

object KafkaConsumerFeatureSourceFactory {

  def apply(brokers: String, zk: String, params: ju.Map[String, Serializable]): FeatureSourceFactory = {

    lazy val expirationPeriod: Option[Long] = {
      Option(KafkaDataStoreFactoryParams.EXPIRATION_PERIOD.lookUp(params)).map(_.toString.toLong).filter(_ > 0)
    }

    val cleanUpCache: Boolean = {
      Option(KafkaDataStoreFactoryParams.CLEANUP_LIVE_CACHE.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
    }

    val useCQCache: Boolean = {
      Option(KafkaDataStoreFactoryParams.USE_CQ_LIVE_CACHE.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
    }

    val monitor: Boolean = {
      Option(KafkaDataStoreFactoryParams.COLLECT_QUERY_STAT.lookUp(params).asInstanceOf[Boolean]).getOrElse(false)
    }

    (entry: ContentEntry, query: Query, schemaManager: KafkaDataStoreSchemaManager) => {
      val kf = new KafkaConsumerFactory(brokers, zk)
      val fc = schemaManager.getFeatureConfig(entry.getTypeName)

      fc.replayConfig match {
        case None =>
          new LiveKafkaConsumerFeatureSource(entry, fc.sft, fc.topic, kf, expirationPeriod, cleanUpCache, useCQCache, query, monitor)

        case Some(rc) =>
          val replaySFT = fc.sft
          val liveSFT = schemaManager.getLiveFeatureType(replaySFT)
            .getOrElse(throw new IllegalArgumentException(
              "Cannot create Replay FeatureSource because SFT has not been properly prepared."))
          new ReplayKafkaConsumerFeatureSource(entry, replaySFT, liveSFT, fc.topic, kf, rc, query)
      }
    }
  }
}
