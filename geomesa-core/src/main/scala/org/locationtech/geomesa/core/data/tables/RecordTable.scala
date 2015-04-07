/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.core.data.tables

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.locationtech.geomesa.core.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.feature.SimpleFeatureEncoder
import org.opengis.feature.simple.SimpleFeature

// TODO: Implement as traits and cache results to gain flexibility and speed-up.
// https://geomesa.atlassian.net/browse/GEOMESA-344
object RecordTable extends GeoMesaTable {

  /** Creates a function to write a feature to the Record Table **/
  def recordWriter(bw: BatchWriter, rowIdPrefix: String): FeatureWriterFn = {
    (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.put(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility, toWrite.dataValue)
      bw.addMutation(m)
    }
  }

  def recordDeleter(bw: BatchWriter, rowIdPrefix: String): FeatureWriterFn = {
    (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.putDelete(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility)
      bw.addMutation(m)
    }
  }

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id
}
