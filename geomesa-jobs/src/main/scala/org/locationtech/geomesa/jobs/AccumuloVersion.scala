package org.locationtech.geomesa.jobs

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.client.mapred.AccumuloInputFormat
import org.apache.hadoop.mapreduce.Job

import scala.util.{Failure, Success, Try}

object AccumuloVersion extends Enumeration {
  type AccumuloVersion = Value
  val V15, V16, V17 = Value

  lazy val accumuloVersion: AccumuloVersion = {
    Try(classOf[AccumuloInputFormat].getMethod("addIterator", classOf[Job], classOf[IteratorSetting])) match {
      case Failure(t: NoSuchMethodException) => V15
      case Success(m)                        => V16
    }
  }
}