/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats.usage

import java.io.Closeable
import java.util.ServiceLoader

trait UsageStatWriter extends Closeable {
  def queueStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit
}

object UsageStatWriter {
  def getUsageStatWriter: Option[UsageStatWriter] = {

    val writers = ServiceLoader.load(classOf[UsageStatWriter]).iterator()

    if (writers.hasNext) {
      val ret = writers.next
      println(s"Found a Service loaded $ret")
      Some(ret)
    } else {
      None
    }
  }
}

