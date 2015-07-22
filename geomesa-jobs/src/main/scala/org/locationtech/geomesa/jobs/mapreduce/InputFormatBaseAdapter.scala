package org.locationtech.geomesa.jobs.mapreduce

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.mapreduce.Job
import org.locationtech.geomesa.accumulo.AccumuloVersion
import AccumuloVersion._

object InputFormatBaseAdapter {

//  def addIterator(job: Job, cfg: IteratorSetting) = accumuloVersion match {
//    case V15 => addIterator15(job, cfg)
//    case V16 => addIterator16(job, cfg)
//  }
//
//  private def addIterator15(job: Job, cfg: IteratorSetting) = {
//    //val method = classOf[InputFormatBase].getMethod("addIterator", classOf[])
//  }
//
//  private def addIterator16(job: Job, cfg: IteratorSetting) = {
//    val method = classOf[AccumuloInputFormat].getMethod("addIterator", classOf[Job], classOf[IteratorSetting])
//    method.invoke(null, job, cfg)
//  }

  def setConnectorInfo(job: Job, user: String, token: PasswordToken) = accumuloVersion match {
    case V15 => setConnectorInfo15(job, user, token)
    case V16 => setConnectorInfo16(job, user, token)
  }

  def setConnectorInfo15(job: Job, user: String, token: PasswordToken) = {
    val method = classOf[AccumuloInputFormat].getMethod("setConnectorInfo", classOf[Job], classOf[String], classOf[PasswordToken])
    method.invoke(null, job, user, token)
  }

  def setConnectorInfo16(job: Job, user: String, token: PasswordToken) = {
    val method = classOf[AccumuloInputFormat].getMethod("setConnectorInfo", classOf[Job], classOf[String], classOf[PasswordToken])
    method.invoke(null, job, user, token)
  }

  def setZooKeeperInstance(job: Job, instance: String, zookeepers: String) = accumuloVersion match {
    case V15 => setZooKeeperInstance15(job, instance, zookeepers)
    case V16 => setZooKeeperInstance16(job, instance, zookeepers)
  }

  def setZooKeeperInstance15(job: Job, instance: String, zookeepers: String) = {
    val method = classOf[AccumuloInputFormat].getMethod("setZooKeeperInstance", classOf[Job], classOf[String], classOf[String])
    method.invoke(null, job, instance, zookeepers)
  }


  def setZooKeeperInstance16(job: Job, instance: String, zookeepers: String) = {
    val method = classOf[AccumuloInputFormat].getMethod("setZooKeeperInstance", classOf[Job], classOf[String], classOf[String])
    method.invoke(null, job, instance, zookeepers)
  }

  def setScanAuthorizations(job: Job, authorizations: Authorizations): Unit = accumuloVersion match {
    case V15 => setScanAuthorizations15(job, authorizations)
    case V16 => setScanAuthorizations16(job, authorizations)
  }

  def setScanAuthorizations15(job: Job, authorizations: Authorizations): Unit = {
    val method = classOf[AccumuloInputFormat].getMethod("setScanAuthorizations", classOf[Job], classOf[Authorizations], classOf[String])
    method.invoke(null, job, authorizations)
  }

  def setScanAuthorizations16(job: Job, authorizations: Authorizations): Unit = {
    val method = classOf[AccumuloInputFormat].getMethod("setScanAuthorizations", classOf[Job], classOf[Authorizations], classOf[String])
    method.invoke(null, job, authorizations)
  }

}
