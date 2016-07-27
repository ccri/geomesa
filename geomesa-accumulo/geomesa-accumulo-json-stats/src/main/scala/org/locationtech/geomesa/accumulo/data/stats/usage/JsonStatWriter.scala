package org.locationtech.geomesa.accumulo.data.stats.usage

import java.io.{File, FileOutputStream, PrintWriter}

import com.google.gson.GsonBuilder

class JsonStatWriter extends UsageStatWriter {
  //def queueStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit

  //def queueStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit
  override def writeStat[T <: UsageStat](stat: T): Unit = {

    val gson = new GsonBuilder()
      .serializeNulls()
      .create()
    println(s"Supposed to write a stat to a file! ${gson.toJson(stat)}")
    val out = new PrintWriter(new FileOutputStream(new File("/tmp/json-logs/qs.log"),true))
    out.write(gson.toJson(stat)+"\n")
    out.close()
  }

  override def close(): Unit = {
    println("Close has been called")

  }
}
