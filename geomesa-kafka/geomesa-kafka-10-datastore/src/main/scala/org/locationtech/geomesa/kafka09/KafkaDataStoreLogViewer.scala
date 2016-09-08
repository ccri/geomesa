/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka09

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.beust.jcommander.{IParameterValidator, JCommander, Parameter, ParameterException}
import com.typesafe.scalalogging.LazyLogging
import kafka.common.MessageFormatter
import kafka.tools.ConsoleConsumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/** An alternative to the kafka-console-consumer providing options specific to viewing the log messages
  * internal to the [[KafkaDataStore]].
  *
  * To run, first copy the geomesa-kafka-gs-plugin.jar to $KAFKA_HOME/libs.  Then create a copy of
  * $KAFKA_HOME/bin/kafka-console-consumer.sh called "kafka-ds-log-viewer" and in the copy replace the
  * classname in the exec command at the end of the script with
  * org.locationtech.geomesa.kafka.KafkaDataStoreLogViewer.
  */


/** A [[MessageFormatter]] that can be used with the kafka-console-consumer to format the internal log
  * messages of the [[KafkaDataStore]]
  *
  * To use add arguments:
  *   --formatter org.locationtech.geomesa.kafka.KafkaGeoMessageFormatter
  *   --property sft.name={sftName}
  *   --property sft.spec={sftSpec}
  *
  * In order to pass the spec via a command argument all "%" characters must be replaced by "%37" and all
  * "=" characters must be replaced by "%61".
  *
  * @see KafkaDataStoreLogViewer for an alternative to kafka-console-consumer
  */
class KafkaGeoMessageFormatter extends MessageFormatter {

  import KafkaGeoMessageFormatter._

  private var decoder: KafkaGeoMessageDecoder = null

  override def init(props: Properties): Unit = {
    if(!props.containsKey(sftNameKey)) {
      throw new IllegalArgumentException(s"Property '$sftNameKey' is required.")
    }

    if(!props.containsKey(sftSpecKey)) {
      throw new IllegalArgumentException(s"Property '$sftSpecKey' is required.")
    }

    val name = props.getProperty(sftNameKey)
    val spec = props.getProperty(sftSpecKey)

    val sft = KafkaDataStoreLogViewer.decodeSFT(name, spec)
    decoder = new KafkaGeoMessageDecoder(sft)
  }

  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
    val msg = decoder.decode(consumerRecord.key, consumerRecord.value)

    output.write(msg.toString.getBytes(StandardCharsets.UTF_8))
    output.write(lineSeparator)
  }

  override def close(): Unit = {
    decoder = null
  }
}