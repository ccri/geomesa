/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.util

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item}
import com.amazonaws.services.dynamodbv2.model._
import com.google.common.collect.Lists
import org.geotools.data.Transaction
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

class DynamoDBDataStore(catalog: String, dynamoDB: DynamoDB) extends ContentDataStore {

  private val CATALOG_TABLE = catalog
  private val catalogTable = getOrCreateCatalogTable(dynamoDB, CATALOG_TABLE)

  private def getOrCreateCatalogTable(dynamoDB: DynamoDB, table: String) = {
    val tables = dynamoDB.listTables().iterator().toList
    val ret = tables
      .find(_.getTableName == table)
      .getOrElse(
        dynamoDB.createTable(
          table,
          util.Arrays.asList(new KeySchemaElement("feature", KeyType.HASH)),
          util.Arrays.asList(new AttributeDefinition("feature", ScalarAttributeType.S)),
          new ProvisionedThroughput()
            .withReadCapacityUnits(5L)
            .withWriteCapacityUnits(6L)))
    ret.waitForActive()
    ret
  }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    import java.{lang => jl}
    val name = featureType.getTypeName

    val attrDefs =
      featureType.getAttributeDescriptors.map { attr =>
        attr.getType.getBinding match {
          case c if c.equals(classOf[String]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.S)

          case c if c.equals(classOf[jl.Integer]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if c.equals(classOf[jl.Double]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if c.equals(classOf[java.util.Date]) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.N)

          case c if classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(c) =>
            new AttributeDefinition()
              .withAttributeName(attr.getLocalName)
              .withAttributeType(ScalarAttributeType.B)
        }
      }

    val keySchema =
      List(
        new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH),
        new KeySchemaElement().withAttributeName("z3").withKeyType(KeyType.RANGE)
      )

    val tableDesc =
      new CreateTableRequest()
        .withTableName(s"${catalog}_${name}_z3")
        .withKeySchema(keySchema)
        .withAttributeDefinitions(
          Lists.newArrayList(
            new AttributeDefinition("id", ScalarAttributeType.S),
            new AttributeDefinition("z3", ScalarAttributeType.N)
          )
        )
        .withProvisionedThroughput(new ProvisionedThroughput(5L, 6L))

    // create the z3 index
    val res = dynamoDB.createTable(tableDesc)

    // write the meta-data
    val metaEntry = new Item().withPrimaryKey("feature", name).withString("sft", SimpleFeatureTypes.encodeType(featureType))
    catalogTable.putItem(metaEntry)

    res.waitForActive()
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val sft =
      Option(entry.getState(Transaction.AUTO_COMMIT).getFeatureType).getOrElse { getSchema(entry) }
    val table = dynamoDB.getTable(s"${catalog}_${sft.getTypeName}_z3")
    new DynamoDBFeatureSource(entry, sft, table)
  }

  def getSchema(entry: ContentEntry) = {
    val item = catalogTable.getItem("feature", entry.getTypeName)
    SimpleFeatureTypes.createType(entry.getTypeName, item.getString("sft"))
  }

  override def createTypeNames(): util.List[Name] = {
    // read types from catalog
    catalogTable.scan().iterator().map { i => new NameImpl(i.getString("feature")) }.toList
  }

}


