/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.{FeatureEvent, FeatureListener}

class TestLambdaFeatureListener(lambda: KafkaFeatureEvent => Unit) extends FeatureListener with Logging {
  override def changed(featureEvent: FeatureEvent): Unit = {
    featureEvent match {
      case kfe: KafkaFeatureEvent =>
        lambda(kfe)
      case _ =>
        logger.debug(s"Received event ${featureEvent.getType}: ${featureEvent.toString}.")
    }
  }
}
