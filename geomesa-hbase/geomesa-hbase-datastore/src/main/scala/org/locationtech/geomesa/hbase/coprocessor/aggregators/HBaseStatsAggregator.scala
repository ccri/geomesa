/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.coprocessor.aggregators

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.commons.codec.binary.Base64
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.hbase.coprocessor.SFT_OPT
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.index.utils.KryoLazyDensityUtils._
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.stats.{Stat, StatSerializer}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable

class HBaseStatsAggregator extends GeoMesaHBaseAggregator {
  import HBaseStatsAggregator._

  var serializer: StatSerializer = _
  var sft: SimpleFeatureType = _

  var internalResult: Stat = _

  override def init(options: Map[String, String]): Unit = {
    sft = IteratorCache.sft(options(SFT_OPT))
    val transformSchema = options.get(TRANSFORM_SCHEMA_OPT).map(IteratorCache.sft).getOrElse(sft)
    serializer = StatSerializer(transformSchema)

    internalResult = Stat(transformSchema, options(STATS_STRING_KEY))
  }

  override def aggregate(sf: SimpleFeature): Unit = internalResult.observe(sf)

  override def encodeResult(): Array[Byte] = serializer.serialize(internalResult)
}

object HBaseStatsAggregator {

  import org.locationtech.geomesa.index.conf.QueryHints.STATS_STRING

  val TRANSFORM_DEFINITIONS_OPT = "tdefs"
  val TRANSFORM_SCHEMA_OPT      = "tsft"
  val STATS_STRING_KEY          = "geomesa.stats.string"
  val STATS_FEATURE_TYPE_KEY    = "geomesa.stats.featuretype"

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", KryoLazyStatsUtils.StatsSft)
    sf.setAttribute(0, Base64.encodeBase64URLSafeString(bytes))
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    sf
  }

  def configure(sft: SimpleFeatureType,
                hints: Hints): Map[String, String] = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val is = mutable.Map.empty[String, String]
    is.put(GeoMesaHBaseAggregator.AGGREGATOR_CLASS, classOf[HBaseStatsAggregator].getName)
    is.put(STATS_STRING_KEY, hints.get(STATS_STRING).asInstanceOf[String])

    val transform = hints.getTransform
    transform.foreach { case (tdef, tsft) =>
      is.put(TRANSFORM_DEFINITIONS_OPT, tdef)
      is.put(TRANSFORM_SCHEMA_OPT, SimpleFeatureTypes.encodeType(tsft))
    }

    is.toMap
  }
}
