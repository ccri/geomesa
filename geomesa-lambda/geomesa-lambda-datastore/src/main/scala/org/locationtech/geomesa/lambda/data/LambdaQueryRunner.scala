/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.LoadingCache
import org.geotools.data.{DataStore, Query}
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.DensityScan
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.utils.{Explainer, KryoLazyStatsUtils}
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class LambdaQueryRunner(persistence: DataStore, transients: LoadingCache[String, TransientStore])
    extends QueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // TODO add query hint for only reading from transient or persistent store
  // note: need to still audit queries if the persistent store isn't hit

  override def runQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    // TODO arrow scans will return two files, will js handle that?

    // if this is a stats query, disable json results temporarily, otherwise we can't merge stats from both sources
    val encodeStats = query.getHints.isStatsQuery && query.getHints.isStatsEncode
    if (!encodeStats) {
      query.getHints.put(QueryHints.ENCODE_STATS, true)
    }

    val transientFeatures = CloseableIterator(transients.get(sft.getTypeName).read(Option(query.getFilter),
      Option(query.getPropertyNames), Option(query.getHints), explain))
    val fc = persistence.getFeatureSource(sft.getTypeName).getFeatures(query)
    // kick off the persistent query in a future, but don't wait for results yet
    val persistentFeatures = Future(CloseableIterator(fc.features))
    // ++ is evaluated lazily, so we will block on the persistent features once the transient iterator is exhausted
    val merged = transientFeatures ++ Await.result(persistentFeatures, Duration.Inf)

    if (query.getHints.isStatsQuery) {
      // for stats queries, merge stats coming from each store so we get the expected one result
      val reduceHints = if (encodeStats) { query.getHints } else {
        // copy the hints so that we don't affect the hints being used by the lazily evaluated persistent query
        val hints = new Hints
        hints.add(query.getHints)
        hints.put(QueryHints.ENCODE_STATS, false)
        hints
      }
      KryoLazyStatsUtils.reduceFeatures(sft, reduceHints)(merged)
    } else {
      merged
    }
  }

  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      KryoLazyStatsUtils.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}
