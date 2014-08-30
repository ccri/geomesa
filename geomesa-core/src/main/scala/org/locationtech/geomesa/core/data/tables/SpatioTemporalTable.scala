/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.core.data.tables

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Mutation, Value, Key}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.index.{IndexEntryEncoder, IndexSchema}
import org.opengis.feature.simple.SimpleFeature
import scala.collection.JavaConverters._

object SpatioTemporalTable {

  def spatioTemporalWriter(bw: BatchWriter, encoder: IndexEntryEncoder): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val KVs = encoder.encode(feature)
      val m = KVs.groupBy { case (k, _) => k.getRow }.map { case (row, kvs) => kvsToMutations(row, kvs) }
      bw.addMutations(m.asJava)
    }

  def kvsToMutations(row: Text, kvs: Seq[(Key, Value)]): Mutation = {
    val m = new Mutation(row)
    kvs.foreach { case (k, v) =>
      m.put(k.getColumnFamily, k.getColumnQualifier, k.getColumnVisibilityParsed, v)
    }
    m
  }

  /** Creates a function to remove spatio temporal index entries for a feature **/
  def removeSpatioTemporalIdx(bw: BatchWriter, encoder: IndexEntryEncoder): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      encoder.encode(feature).foreach { case (key, _) =>
        val m = new Mutation(key.getRow)
        m.putDelete(key.getColumnFamily, key.getColumnQualifier, key.getColumnVisibilityParsed)
        bw.addMutation(m)
      }
    }
}
