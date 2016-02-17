/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.dynamodb.data

import java.util

import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.model._
import com.google.common.collect.Lists
import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.Transaction
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource, ContentState}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

object DynamoDBDataStore {
  val keySchema =
    List(
      new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH),
      new KeySchemaElement().withAttributeName("z3").withKeyType(KeyType.RANGE)
    )

  val attributeDescriptions = Lists.newArrayList(
    new AttributeDefinition("id", ScalarAttributeType.S),
    new AttributeDefinition("z3", ScalarAttributeType.N)
  )

  def makeTableName(catalog: String, name: String): String = s"${catalog}_${name}_z3"

  def getSchema(entry: ContentEntry, catalogTable: Table): SimpleFeatureType  = {
    val item = catalogTable.getItem("feature", entry.getTypeName)
    SimpleFeatureTypes.createType(entry.getTypeName, item.getString("sft"))
  }

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

}

class DynamoDBDataStore(catalog: String, dynamoDB: DynamoDB) extends ContentDataStore with SchemaValidation with LazyLogging {
  import DynamoDBDataStore._

  private val CATALOG_TABLE = catalog
  private val catalogTable: Table = getOrCreateCatalogTable(dynamoDB, CATALOG_TABLE)

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val sft = Option(entry.getState(Transaction.AUTO_COMMIT).getFeatureType).getOrElse { DynamoDBDataStore.getSchema(entry, catalogTable) }
    val table = dynamoDB.getTable(makeTableName(catalog, sft.getTypeName))
    new DynamoDBFeatureStore(entry, sft, table)
  }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    validatingCreateSchema(featureType, createSchemaImpl)
  }

  private def createSchemaImpl(featureType: SimpleFeatureType): Unit = {
    import java.{lang => jl}

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

    val name = featureType.getTypeName
    val tableDesc =
      new CreateTableRequest()
        .withTableName(makeTableName(catalog, name))
        .withKeySchema(keySchema)
        .withAttributeDefinitions(attributeDescriptions)
        .withProvisionedThroughput(new ProvisionedThroughput(5L, 6L))

    // create the z3 index
    val res = dynamoDB.createTable(tableDesc)

    // write the meta-data
    val metaEntry = createDDMMetaDataItem(name, featureType)
    catalogTable.putItem(metaEntry)

    res.waitForActive()
  }

  override def createTypeNames(): util.List[Name] = {
    // read types from catalog
    catalogTable.scan().iterator().map { i => new NameImpl(i.getString("feature")) }.toList
  }

  override def createContentState(entry: ContentEntry): ContentState = {
    new DynamoDBContentState(entry, catalogTable)
  }

  override def dispose(): Unit = if (dynamoDB != null) dynamoDB.shutdown()

  private def createDDMMetaDataItem(name: String, featureType: SimpleFeatureType): Item = {
    new Item().withPrimaryKey("feature", name).withString("sft", SimpleFeatureTypes.encodeType(featureType))
  }

  def updateProvisionedThroughput(name: String, pt: ProvisionedThroughput): Unit = {
    val tableName = makeTableName(catalog, name)
    logger.info("Attempting to Modify provisioned throughput for {}", tableName)
    try {
      val table = dynamoDB.getTable(tableName)
      table.updateTable(pt)
      table.waitForActive()
      logger.info(s"Updated table: $tableName to have ProvisionedThroughput: ${pt.toString}")
    } catch {
      case NonFatal(e) => logger.error(s"Unable to update table: $tableName", e)
    }
  }

}


trait SchemaValidation {

  protected def validatingCreateSchema(featureType: SimpleFeatureType, cs: (SimpleFeatureType) => Unit): Unit = {
    // validate dtg
    featureType.getAttributeDescriptors
      .find { ad => ad.getType.getBinding.isAssignableFrom(classOf[java.util.Date]) }
      .getOrElse(throw new IllegalArgumentException("Could not find a dtg field"))

    // validate geometry
    featureType.getAttributeDescriptors
      .find { ad => ad.getType.getBinding.isAssignableFrom(classOf[Geometry]) }
      .getOrElse(throw new IllegalArgumentException("Could not find a valid point geometry"))

    cs(featureType)
  }

}

