package org.locationtech.geomesa.accumulo.data.stats.usage

import java.io.Closeable
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

trait UsageStatWriter extends Closeable {
  def queueStat[T <: UsageStat](stat: T)(implicit transform: UsageStatTransform[T]): Unit
}

object UsageStatWriter extends ApplicationContextAware  {

  var context: ApplicationContext = null

  def getUsageStatWriter: Option[UsageStatWriter] = {

    val writers = context.getBeansOfType(classOf[UsageStatWriter]).values().iterator()

    if (writers.hasNext) {
      val ret = writers.next
      println(s"Found a Service loaded $ret")
      Some(ret)
    } else {
      None
    }
  }

  override def setApplicationContext(applicationContext: ApplicationContext): Unit = {
    println("*** In Set ApplicationContext.")
    this.context = applicationContext
  }
}

