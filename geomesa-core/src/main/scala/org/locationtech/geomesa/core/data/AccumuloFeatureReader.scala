/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.core.data

import org.geotools.data.{FeatureReader, Query}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.stats._
import org.locationtech.geomesa.core.util.ExplainingConnectorCreator
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.SimpleFeatureEncoder
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(dataStore: AccumuloDataStore,
                            query: Query,
                            sft: SimpleFeatureType,
                            indexSchemaFmt: String,
                            featureEncoding: FeatureEncoding,
                            version: Int)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  implicit val timings = new TimingsImpl

  private val hints = dataStore.strategyHints(sft)
  private val planner = new QueryPlanner(sft, featureEncoding, indexSchemaFmt, dataStore, hints, version)
  private val iter = profile(planner.query(query), "planning")

  override def getFeatureType = sft

  override def next() = profile(iter.next(), "next")

  override def hasNext = profile(iter.hasNext, "hasNext")

  override def close() = {
    iter.close()

    dataStore match {
      case sw: StatWriter =>
        val stat =
          QueryStat(sft.getTypeName,
                    System.currentTimeMillis(),
                    QueryStatTransform.filterToString(query.getFilter),
                    QueryStatTransform.hintsToString(query.getHints),
                    timings.time("planning"),
                    timings.time("next") + timings.time("hasNext"),
                    timings.occurrences("next").toInt)
        sw.writeStat(stat, dataStore.getQueriesTableName(sft))
      case _ => // do nothing
    }
  }
}

class AccumuloQueryExplainer(dataStore: AccumuloDataStore,
                             query: Query,
                             sft: SimpleFeatureType,
                             indexSchemaFmt: String,
                             featureEncoding: FeatureEncoding,
                             version: Int) extends MethodProfiling {

  def explainQuery(o: ExplainerOutputType) = {
    implicit val timings = new TimingsImpl
    profile(planQuery(o), "plan")
    o(s"Query Planning took ${timings.time("plan")} milliseconds.")
  }

  private def planQuery(o: ExplainerOutputType) = {
    val cc = new ExplainingConnectorCreator(o)
    val hints = dataStore.strategyHints(sft)
    val qp = new QueryPlanner(sft, featureEncoding, indexSchemaFmt, cc, hints, version)
    qp.planQuery(query, o)
  }
}
