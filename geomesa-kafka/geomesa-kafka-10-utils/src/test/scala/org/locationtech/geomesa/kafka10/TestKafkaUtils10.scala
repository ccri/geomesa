/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka10

import java.util.Properties

import _root_.kafka.utils.TestUtils
import org.locationtech.geomesa.kafka09.TestKafkaUtils

class TestKafkaUtils10 extends TestKafkaUtils {
  override def createBrokerConfig(nodeId: Int, zkConnect: String): Properties = TestUtils.createBrokerConfig(nodeId, zkConnect)
  override def choosePort: Int = TestUtils.RandomPort
}
