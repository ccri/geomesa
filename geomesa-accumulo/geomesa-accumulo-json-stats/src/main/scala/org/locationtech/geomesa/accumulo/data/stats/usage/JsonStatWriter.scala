package org.locationtech.geomesa.accumulo.data.stats.usage

import org.locationtech.geomesa.accumulo.data.stats.usage


class JsonStatWriter extends UsageStatWriter {
  //def queueStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit

  //def queueStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit
  override def writeStat[T <: UsageStat](stat: T): Unit = {
    println(s"Supposed to write a stat to a file! $stat")

  }

  override def close(): Unit = {
    println("Close has been called")

  }
}
