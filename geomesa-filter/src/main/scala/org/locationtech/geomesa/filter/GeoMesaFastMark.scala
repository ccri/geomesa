/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.filter

import org.geotools.filter.FunctionExpressionImpl
import org.geotools.filter.capability.FunctionNameImpl
import org.geotools.filter.capability.FunctionNameImpl._

class GeoMesaFastMark extends FunctionExpressionImpl(
  new FunctionNameImpl("geomesaFastMark",
  parameter("geomesaFastMark", classOf[String]),
  parameter("icon", classOf[String]),
  parameter("rotation", classOf[Double]))) {

  override def evaluate(o: java.lang.Object): AnyRef = {
    val icon = getExpression(0).evaluate(o).asInstanceOf[java.lang.String]
    val rotation: java.lang.Double = Option(getExpression(1).evaluate(o).asInstanceOf[java.lang.Double]).getOrElse(0.0d)
    (icon, rotation)
  }
}