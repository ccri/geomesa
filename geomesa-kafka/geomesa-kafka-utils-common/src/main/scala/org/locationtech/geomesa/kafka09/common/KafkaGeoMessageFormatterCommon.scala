package org.locationtech.geomesa.kafka09.common

import java.io.PrintStream
import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord

trait KafkaGeoMessageFormatterCommon {
  def init(prop: Properties): Unit
  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream): Unit = ???
  def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]],output: PrintStream): Unit = ???
  def close(): Unit
}
