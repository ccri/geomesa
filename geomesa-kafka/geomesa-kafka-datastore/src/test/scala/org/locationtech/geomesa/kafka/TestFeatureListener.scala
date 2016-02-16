package org.locationtech.geomesa.kafka

import org.geotools.data.{FeatureEvent, FeatureListener}

class TestFeatureListener extends FeatureListener {
  override def changed(featureEvent: FeatureEvent): Unit = {
    println(s"Received event ${featureEvent.getType}: ${featureEvent.toString}.")


    featureEvent.getFilter

  }
}
