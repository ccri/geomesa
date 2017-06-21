/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream

import java.io.Closeable

import org.locationtech.geomesa.index.utils.{DistributedLocking, Releasable}
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener

/**
  * Manages storing and watching distributed offsets
  */
trait OffsetManager extends DistributedLocking with Closeable {
  def getOffsets(topic: String): Seq[(Int, Long)]
  def setOffsets(topic: String, offsets: Seq[(Int, Long)]): Unit
  def deleteOffsets(topic: String): Unit
  def addOffsetListener(topic: String, listener: OffsetListener): Unit
  def removeOffsetListener(topic: String, listener: OffsetListener): Unit
  def acquireLock(topic: String, partition: Int, timeOut: Long): Option[Releasable] =
    acquireDistributedLock(s"$topic/$partition", timeOut)
}

object OffsetManager {
  trait OffsetListener {
    def offsetsChanged(offsets: Seq[(Int, Long)]): Unit
  }
}
