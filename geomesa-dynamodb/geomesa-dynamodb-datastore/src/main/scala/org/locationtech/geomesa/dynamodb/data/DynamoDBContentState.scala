/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import com.amazonaws.services.dynamodbv2.document.Table
import org.geotools.data.store.{ContentEntry, ContentState}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.utils.text.ObjectPoolFactory
import org.opengis.feature.simple.SimpleFeatureType

class DynamoDBContentState(entry: ContentEntry, catalog: Table) extends ContentState(entry) {

  val sft: SimpleFeatureType = DynamoDBDataStore.getSchema(entry, catalog)
  val builderPool = ObjectPoolFactory(getBuilder, 10)
  
  private def getBuilder = {
    val builder = new SimpleFeatureBuilder(sft)
    builder.setValidating(java.lang.Boolean.FALSE)
    builder
  }

}
