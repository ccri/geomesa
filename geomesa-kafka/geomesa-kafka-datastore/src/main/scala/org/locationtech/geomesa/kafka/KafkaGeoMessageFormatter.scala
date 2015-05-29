/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.kafka

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import java.util.Properties

import com.typesafe.scalalogging.slf4j.Logging
import joptsimple.{OptionParser, OptionSet}
import kafka.tools.{ConsoleConsumer, MessageFormatter}
import kafka.utils.CommandLineUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

/** A [[MessageFormatter]] that can be used with the kafka-console-consumer to format the internal log
  * messages of the [[KafkaDataStore]]
  *
  * To use add arguments:
  *   --formatter org.locationtech.geomesa.kafka.KafkaGeoMessageFormatter
  *   --property sft.name={sftName}
  *   --property sft.spec={sftSpec}
  *
  * In order to pass the spec via a command argument all "=" characters must be replaced by "%61".  Any
  * "%" characters that exist prior to replacement of "=" must be replaced by "%37".
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

  override def writeTo(key: Array[Byte], value: Array[Byte], output: PrintStream): Unit = {
    val msg = decoder.decode(key, value)

    output.write(msg.toString.getBytes(StandardCharsets.UTF_8))
    output.write(lineSeparator)
  }

  override def close(): Unit = {
    decoder = null
  }
}

/** An alternative to the kafka-console-consumer providing options specific to viewing the log messages
  * internal to the [[KafkaDataStore]].
  *
  * To run, first copy the geomesa-kafka-geoserver-plugin.jar to $KAFKA_HOME/libs.  Then create a copy of
  * $KAFKA_HOME/bin/kafka-console-consumer.sh called "kafka-ds-log-viewer" and in the copy replace the
  * classname in the exec command at the end of the script with
  * org.locationtech.geomesa.kafka.KafkaDataStoreLogViewer.
  */
object KafkaDataStoreLogViewer extends Logging {

  import ConsoleConsumer._
  import KafkaGeoMessageFormatter._

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser

    val zkConnectOpt = parser.accepts("zookeeper",
      "REQUIRED: The connection string for the zookeeper connection in the form host:port. " +
      "Multiple URLS can be given to allow fail-over.")
      .withRequiredArg
      .describedAs("urls")
      .ofType(classOf[String])

    val zkPathOpt = parser.accepts("zkPath",
      "REQUIRED: The base zkPath.  Must match zkPath used to configure the Kafka Data Store.")
      .withRequiredArg
      .describedAs("string")
      .ofType(classOf[String])

    val sftNameOpt = parser.accepts("sft",
      "REQUIRED: The name of Simple Feature Type.")
      .withRequiredArg
      .describedAs("string")
      .ofType(classOf[String])

    val fromOpt = parser.accepts("from",
      "OPTIONAL: Where to start reading from.  Defaults to oldest.")
      .withOptionalArg
      .describedAs("oldest|newest")
      .ofType(classOf[String])

    val options: OptionSet = tryParse(parser, args)
    CommandLineUtils.checkRequiredArgs(parser, options, zkConnectOpt, zkPathOpt, sftNameOpt)

    val zookeepers = options.valueOf(zkConnectOpt)
    val zkPath = options.valueOf(zkPathOpt)
    val sftName = options.valueOf(sftNameOpt)

    // TODO support from oldest+n, oldest+t, newest-n, newest-t, time=t, offset=o
    val fromBeginning = {
      if (options.has(fromOpt)) {
        val from = options.valueOf(fromOpt)
        if ("oldest".equalsIgnoreCase(from)) {
          true
        } else if ("newest".equalsIgnoreCase(from)) {
          false
        } else {
          throw new IllegalArgumentException("Invalid 'from' option.  Legal values are 'oldest' and 'newest'")
        }
      } else {
        true // default
      }
    }

    run(zookeepers, zkPath, sftName, fromBeginning)
  }

  def run(zookeeper: String, zkPath: String, sftName: String, fromBeginning: Boolean): Unit = {
    val featureConfig = new KafkaDataStore(zookeeper, zkPath, 1, 1, null).getFeatureConfig(sftName)

    val formatter = classOf[KafkaGeoMessageFormatter].getName
    val sftSpec = encodeSFT(featureConfig.sft)

    var ccArgs = Seq("--topic", featureConfig.topic,
                       "--zookeeper", zookeeper,
                       "--formatter", formatter,
                       "--property", s"$sftNameKey=$sftName",
                       "--property", s"$sftSpecKey=$sftSpec")

    if (fromBeginning) {
      ccArgs :+= "--from-beginning"
    }

    val ccClass = Class.forName("kafka.tools.ConsoleConsumer")
    ConsoleConsumer.main(ccArgs.toArray)
  }

  // double encode so that spec can be passed via command line
  def encodeSFT(sft: SimpleFeatureType): String =
    SimpleFeatureTypes.encodeType(sft).replaceAll("%", "%37").replaceAll("=", "%61")

  def decodeSFT(name: String, spec: String): SimpleFeatureType =
    SimpleFeatureTypes.createType(name, spec.replaceAll("%61", "=").replaceAll("%37", "%"))
}

object KafkaGeoMessageFormatter {
  private[kafka] val sftNameKey = "sft.name"
  private[kafka] val sftSpecKey = "sft.spec"

  val lineSeparator = "\n".getBytes(StandardCharsets.UTF_8)
}