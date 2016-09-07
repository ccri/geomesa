package org.locationtech.geomesa.kafka10

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import kafka.common.MessageFormatter
import org.apache.kafka.clients.consumer.ConsumerRecord

/**
  * Created by ckelly on 9/6/16.
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

  def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream): Unit = {
    val msg = decoder.decode(key, value)

    output.write(msg.toString.getBytes(StandardCharsets.UTF_8))
    output.write(lineSeparator)
  }
  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit =
    writeTo(consumerRecord.key(), consumerRecord.value(), output)
  override def close(): Unit = {
    decoder = null
  }

}
object KafkaGeoMessageFormatter {
  private[kafka] val sftNameKey = "sft.name"
  private[kafka] val sftSpecKey = "sft.spec"

  val lineSeparator = "\n".getBytes(StandardCharsets.UTF_8)
}
