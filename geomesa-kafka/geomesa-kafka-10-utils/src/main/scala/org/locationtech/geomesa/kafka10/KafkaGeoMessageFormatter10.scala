/***********************************************************************
  * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0
  * which accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.kafka10

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import kafka.common.MessageFormatter
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.locationtech.geomesa.kafka.common.KafkaGeoMessageFormatterCommon
import org.locationtech.geomesa.kafka.{KafkaDataStoreLogViewer, KafkaGeoMessageDecoder}

/**
  * Created by ckelly on 9/7/16.
  */
class KafkaGeoMessageFormatter10 extends MessageFormatter with KafkaGeoMessageFormatterCommon{

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

  override def close: Unit = {
    decoder = null
  }
}

object KafkaGeoMessageFormatter {
  private[kafka] val sftNameKey = "sft.name"
  private[kafka] val sftSpecKey = "sft.spec"

  val lineSeparator = "\n".getBytes(StandardCharsets.UTF_8)
}
