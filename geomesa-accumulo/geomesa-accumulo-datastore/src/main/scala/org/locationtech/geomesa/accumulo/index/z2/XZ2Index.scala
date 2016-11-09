/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z2

import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.index.{DefaultConfig, IndexConfig}
import org.opengis.feature.simple.SimpleFeatureType

case class XZ2Config(var numSplits: Int = XZ2Index.DEFAULT_NUM_SPLITS) extends IndexConfig {
  val splitArrays = (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toSeq
}

case class XZ2Index(conf: IndexConfig = DefaultConfig)
  extends AccumuloFeatureIndexType with IndexConfig with XZ2WritableIndex with XZ2QueryableIndex {

  var numSplits = conf.numSplits
  val splitArrays = conf.splitArrays

  override val name: String = XZ2Index.name

  override val version: Int = XZ2Index.version

  override val serializedWithId: Boolean = XZ2Index.serializedWithId

  override def supports(sft: SimpleFeatureType): Boolean = XZ2Index.supports(sft)

}

case object XZ2Index extends AccumuloFeatureIndexType with IndexConfig with XZ2WritableIndex with XZ2QueryableIndex {

  override val name: String = "xz2"

  override val version: Int = 1

  override val serializedWithId: Boolean = false

  override def supports(sft: SimpleFeatureType): Boolean = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    sft.nonPoints
  }
  val DEFAULT_NUM_SPLITS = 4
  override var numSplits: Int = DEFAULT_NUM_SPLITS
  val splitArrays = (0 until numSplits).map(_.toByte).toArray.map(Array(_)).toSeq
}
