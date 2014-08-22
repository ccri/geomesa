package org.locationtech.geomesa.core.data.tables

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Range, Value, Mutation}
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.core.data._
import org.opengis.feature.simple.SimpleFeature

object RecordTable {


  def buildWrite(encoder: SimpleFeatureEncoder, visibility: String): SimpleFeature => Mutation =
    (feature: SimpleFeature) => {
      val m = new Mutation(feature.getID)
      m.put(SFT_CF, EMPTY_COLQ, new ColumnVisibility(visibility), new Value(encoder.encode(feature)))
      m
  }

  def buildDelete(encoder: SimpleFeatureEncoder, visibility: String): SimpleFeature => Mutation =
    (feature: SimpleFeature) => {
      val m = new Mutation(feature.getID)
      m.putDelete(SFT_CF, EMPTY_COLQ, new ColumnVisibility(visibility))
      m
    }

  /** Creates a function to write a feature to the Record Table **/
  def recordWriter(bw: BatchWriter, encoder: SimpleFeatureEncoder, visibility: String): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val builder = buildWrite(encoder, visibility)
      bw.addMutation(builder(feature))
    }

  def recordDeleter(bw: BatchWriter, encoder: SimpleFeatureEncoder, visibility: String): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val builder = buildDelete(encoder, visibility)
      bw.addMutation(builder(feature))
    }



  /** Creates a function to remove a feature from the record table **/
//  private def removeRecord(bw: BatchWriter): SimpleFeature => Unit =
//    (feature: SimpleFeature) => {
//      val row = new Text(feature.getID)
//      val mutation = new Mutation(row)
//
//      val scanner = dataStore.createRecordScanner(featureType)
//      scanner.setRanges(List(new ARange(row, true, row, true)))
//      scanner.iterator().foreach { entry =>
//        val key = entry.getKey
//        mutation.putDelete(key.getColumnFamily, key.getColumnQualifier, key.getColumnVisibilityParsed)
//      }
//      if (mutation.size() > 0) {
//        bw.addMutation(mutation)
//      }
//    }

}
