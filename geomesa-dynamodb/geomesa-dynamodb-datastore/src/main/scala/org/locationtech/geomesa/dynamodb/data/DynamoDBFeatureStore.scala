/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.document.Table
import org.geotools.data.simple.DelegateSimpleFeatureReader
import org.geotools.data.store.{ContentEntry, ContentFeatureStore}
import org.geotools.data.{FeatureReader, FeatureWriter, Query}
import org.geotools.feature.collection.DelegateSimpleFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.filter.FilterHelper
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.GenTraversable
import scala.collection.JavaConversions._

class DynamoDBFeatureStore(entry: ContentEntry,
                           sft: SimpleFeatureType,
                           table: Table)
  extends ContentFeatureStore(entry, Query.ALL) {

  private lazy val contentState: DynamoDBContentState = entry.getState(getTransaction).asInstanceOf[DynamoDBContentState]

  override def buildFeatureType(): SimpleFeatureType = contentState.sft

  override def getBoundsInternal(query: Query): ReferencedEnvelope = new ReferencedEnvelope(-180.0, 180.0, -90.0, 90.0, DefaultGeographicCRS.WGS84)

  override def getCountInternal(query: Query): Int = {
    // TODO: getItemCount returns a Long, may need to do something safer
    table.getDescription.getItemCount.toInt
  }

  override def getReaderInternal(query: Query): FeatureReader[SimpleFeatureType, SimpleFeature] = {
    val iter: Iterator[SimpleFeature] =
      if(query.equals(Query.ALL) || FilterHelper.isFilterWholeWorld(query.getFilter)) {
        getAllFeatures
      } else {
        val plans    = planQuery(query)
        val features = executeGeoTimeQuery(query, plans)
        features
      }
    new DelegateSimpleFeatureReader(contentState.sft, new DelegateSimpleFeatureIterator(iter))
  }

  override def getWriterInternal(query: Query, flags: Int): FeatureWriter[SimpleFeatureType, SimpleFeature] = {
    if((flags | WRITER_ADD) == WRITER_ADD) new DynamoDBAppendingFeatureWriter(contentState.sft, contentState.table)
    else                                   new DynamoDBUpdatingFeatureWriter(contentState.sft, contentState.table)
  }

  def getAllFeatures: Iterator[SimpleFeature] = {
    contentState.table.scan(contentState.ALL_QUERY).iterator().map(i => contentState.serializer.deserialize(i.getBinary("ser")))
  }

  def planQuery(query: Query): GenTraversable[Any] = ???

  def executeGeoTimeQuery(query: Query, plans: GenTraversable[Any]): Iterator[SimpleFeature] = ???

}

