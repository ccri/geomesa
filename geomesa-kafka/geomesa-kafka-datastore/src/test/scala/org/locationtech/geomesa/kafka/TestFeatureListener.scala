package org.locationtech.geomesa.kafka

import org.geotools.data.{FeatureEvent, FeatureListener}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

class TestFeatureListener extends FeatureListener {
  override def changed(featureEvent: FeatureEvent): Unit = {

    println(s"Is: KFE ${featureEvent.isInstanceOf[KafkaFeatureEvent]}")

    featureEvent match {
      case kfe: KafkaFeatureEvent =>
        val feat: SimpleFeature = kfe.feature
        feat.getAttributes.foreach{ println }

      case _ =>
        println(s"Received event ${featureEvent.getType}: ${featureEvent.toString}.")
    }
  }
}
