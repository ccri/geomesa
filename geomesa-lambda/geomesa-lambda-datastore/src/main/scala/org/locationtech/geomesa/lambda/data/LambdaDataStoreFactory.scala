/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import java.awt.RenderingHints.Key
import java.io.{Serializable, StringReader}
import java.time.Clock
import java.util.{Collections, Properties}

import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStoreFactory, AccumuloDataStoreParams}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.lambda.data.LambdaDataStore.LambdaConfig
import org.locationtech.geomesa.lambda.stream.ZookeeperOffsetManager
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class LambdaDataStoreFactory extends DataStoreFactorySpi {

  import LambdaDataStoreFactory.Params._
  import LambdaDataStoreFactory.parseKafkaConfig

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    val brokers = Kafka.BrokersParam.lookUp(params).asInstanceOf[String]
    val expiry = try { Duration(ExpiryParam.lookUp(params).asInstanceOf[String]).toMillis } catch {
      case NonFatal(e) => throw new RuntimeException(s"Couldn't parse expiry parameter: ${ExpiryParam.lookUp(params)}", e)
    }
    val persist = Option(PersistParam.lookUp(params).asInstanceOf[java.lang.Boolean]).forall(_.booleanValue)
    val partitions = Option(Kafka.PartitionsParam.lookUp(params).asInstanceOf[Integer]).map(_.intValue).getOrElse(1)

    val consumerConfig = parseKafkaConfig(Kafka.ConsumerParam.lookUp(params).asInstanceOf[String]) ++
        Map("bootstrap.servers" -> brokers)
    val producer = {
      val producerConfig = parseKafkaConfig(Kafka.ProducerParam.lookUp(params).asInstanceOf[String]) ++
          Map("bootstrap.servers" -> brokers, "num.partitions" -> partitions.toString)
      KafkaStore.producer(producerConfig)
    }

    // TODO audit queries if some don't get sent to accumulo
    val persistence = new AccumuloDataStoreFactory().createDataStore(filter("accumulo", params))

    val zkNamespace = s"gm_lambda_${persistence.config.catalog}"

    val zk = Kafka.ZookeepersParam.lookUp(params).asInstanceOf[String]

    val offsetManager = new ZookeeperOffsetManager(zk, zkNamespace)

    val clock = Option(ClockParam.lookUp(params).asInstanceOf[Clock]).getOrElse(Clock.systemUTC())

    val config = LambdaConfig(zk, zkNamespace, partitions, expiry, persist)

    val ds = new LambdaDataStore(producer, consumerConfig, persistence, offsetManager, config)(clock)

    ds
  }

  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def canProcess(params: java.util.Map[String, Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(filter("accumulo", params)) &&
        Seq(ExpiryParam, Kafka.BrokersParam, Kafka.ZookeepersParam).forall(p => params.containsKey(p.getName))

  override def getParametersInfo: Array[Param] = Array(
    LambdaDataStoreFactory.Params.Accumulo.InstanceParam,
    LambdaDataStoreFactory.Params.Accumulo.ZookeepersParam,
    LambdaDataStoreFactory.Params.Accumulo.CatalogParam,
    LambdaDataStoreFactory.Params.Accumulo.UserParam,
    LambdaDataStoreFactory.Params.Accumulo.PasswordParam,
    LambdaDataStoreFactory.Params.Accumulo.KeytabParam,
    LambdaDataStoreFactory.Params.Kafka.BrokersParam,
    LambdaDataStoreFactory.Params.Kafka.ZookeepersParam,
    LambdaDataStoreFactory.Params.ExpiryParam,
    LambdaDataStoreFactory.Params.PersistParam,
    LambdaDataStoreFactory.Params.Accumulo.AuthsParam,
    LambdaDataStoreFactory.Params.Accumulo.EmptyAuthsParam,
    LambdaDataStoreFactory.Params.Accumulo.VisibilitiesParam,
    LambdaDataStoreFactory.Params.Accumulo.QueryTimeoutParam,
    LambdaDataStoreFactory.Params.Accumulo.QueryThreadsParam,
    LambdaDataStoreFactory.Params.Accumulo.RecordThreadsParam,
    LambdaDataStoreFactory.Params.Accumulo.WriteThreadsParam,
    LambdaDataStoreFactory.Params.Kafka.PartitionsParam,
    LambdaDataStoreFactory.Params.Kafka.ProducerParam,
    LambdaDataStoreFactory.Params.Kafka.ConsumerParam,
    LambdaDataStoreFactory.Params.LooseBBoxParam,
    LambdaDataStoreFactory.Params.GenerateStatsParam,
    LambdaDataStoreFactory.Params.AuditQueriesParam
  )

  override def getDisplayName: String = LambdaDataStoreFactory.DisplayName

  override def getDescription: String = LambdaDataStoreFactory.Description

  override def isAvailable: Boolean = true

  override def getImplementationHints: java.util.Map[Key, _] =
    java.util.Collections.EMPTY_MAP.asInstanceOf[java.util.Map[Key, _]]
}

object LambdaDataStoreFactory {

  object Params {

    object Accumulo {
      val InstanceParam      = copy("accumulo", AccumuloDataStoreParams.instanceIdParam)
      val ZookeepersParam    = copy("accumulo", AccumuloDataStoreParams.zookeepersParam)
      val CatalogParam       = copy("accumulo", AccumuloDataStoreParams.tableNameParam)
      val UserParam          = copy("accumulo", AccumuloDataStoreParams.userParam)
      val PasswordParam      = copy("accumulo", AccumuloDataStoreParams.passwordParam)
      val KeytabParam        = copy("accumulo", AccumuloDataStoreParams.keytabPathParam)
      val AuthsParam         = copy("accumulo", AccumuloDataStoreParams.authsParam)
      val EmptyAuthsParam    = copy("accumulo", AccumuloDataStoreParams.forceEmptyAuthsParam)
      val VisibilitiesParam  = copy("accumulo", AccumuloDataStoreParams.visibilityParam)
      val QueryTimeoutParam  = copy("accumulo", AccumuloDataStoreParams.queryTimeoutParam)
      val QueryThreadsParam  = copy("accumulo", AccumuloDataStoreParams.queryThreadsParam)
      val RecordThreadsParam = copy("accumulo", AccumuloDataStoreParams.recordThreadsParam)
      val WriteThreadsParam  = copy("accumulo", AccumuloDataStoreParams.writeThreadsParam)
      val MockParam          = copy("accumulo", AccumuloDataStoreParams.mockParam)
    }

    object Kafka {
      val BrokersParam    = new Param("kafka.brokers", classOf[String], "Kafka brokers", true)
      val ZookeepersParam = new Param("kafka.zookeepers", classOf[String], "Kafka zookeepers", true)
      val PartitionsParam = new Param("kafka.partitions", classOf[Integer], "Number of partitions to use in kafka queues", false, Int.box(1))
      val ProducerParam   = new Param("kafka.producer.options", classOf[String], "Kafka producer configuration options, in Java properties format", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
      val ConsumerParam   = new Param("kafka.consumer.options", classOf[String], "Kafka consumer configuration options, in Java properties format'", false, null, Collections.singletonMap(Parameter.IS_LARGE_TEXT, java.lang.Boolean.TRUE))
    }

    val ExpiryParam        = new Param("expiry", classOf[String], "Duration before features expire from transient store", true, "1h")
    val PersistParam       = new Param("persist", classOf[java.lang.Boolean], "Whether to persist expired features to long-term storage", false, java.lang.Boolean.TRUE)
    val LooseBBoxParam     = GeoMesaDataStoreFactory.LooseBBoxParam
    val GenerateStatsParam = GeoMesaDataStoreFactory.GenerateStatsParam
    val AuditQueriesParam  = GeoMesaDataStoreFactory.AuditQueriesParam
    val ClockParam         = new Param("clock", classOf[Clock], "Clock instance to use for timing", false)

    private [data] def copy(ns: String, p: Param): Param =
      new Param(s"$ns.${p.key}", p.`type`, p.title, p.description, p.required, p.minOccurs, p.maxOccurs, p.sample, p.metadata)

    private [data] def filter(ns: String, params: java.util.Map[String, Serializable]): java.util.Map[String, Serializable] = {
      // note: includes a bit of redirection to allow us to pass non-serializable values in to tests
      import scala.collection.JavaConverters._
      Map[String, Any](params.asScala.toSeq: _ *)
          .map { case (k, v) => (if (k.startsWith(s"$ns.")) { k.substring(ns.length + 1) } else { k }, v) }
          .asJava.asInstanceOf[java.util.Map[String, Serializable]]
    }
  }

  private val DisplayName = "Accumulo/Kafka Lambda (GeoMesa)"

  private val Description = "Hybrid store using Kafka for recent events and Accumulo for long-term storage"

  def parseKafkaConfig(value: String): Map[String, String] = {
    import scala.collection.JavaConversions._
    if (value == null || value.isEmpty) { Map.empty } else {
      val props = new Properties()
      props.load(new StringReader(value))
      props.entrySet().map(e => e.getKey.asInstanceOf[String] -> e.getValue.asInstanceOf[String]).toMap
    }
  }
}
