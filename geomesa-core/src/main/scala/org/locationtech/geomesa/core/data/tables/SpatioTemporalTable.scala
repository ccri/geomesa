package org.locationtech.geomesa.core.data.tables

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Mutation, Value, Key}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.index.IndexSchema
import org.opengis.feature.simple.SimpleFeature
import scala.collection.JavaConverters._

object SpatioTemporalTable {

  def spatioTemporalWriter(bw: BatchWriter, indexer: IndexSchema): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val KVs = indexer.encode(feature)
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
  def removeSpatioTemporalIdx(bw: BatchWriter, indexer: IndexSchema): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      indexer.encode(feature).foreach { case (key, _) =>
        val m = new Mutation(key.getRow)
        m.putDelete(key.getColumnFamily, key.getColumnQualifier, key.getColumnVisibilityParsed)
        bw.addMutation(m)
      }
    }
}
