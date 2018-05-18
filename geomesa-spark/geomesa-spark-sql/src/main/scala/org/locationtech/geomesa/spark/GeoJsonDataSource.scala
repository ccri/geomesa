/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.{lang => jl}

import com.vividsolutions.jts.geom.Geometry
import com.vividsolutions.jts.io.geojson.GeoJsonReader
import net.minidev.json.{JSONArray, JSONObject}
import net.minidev.json.parser.JSONParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.jts.JTSTypes
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.locationtech.geomesa.tools.utils.GeoJsonInference
import org.locationtech.geomesa.spark.jts._
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._
import scala.collection.mutable

class GeoJsonDataSource extends DataSourceRegister with RelationProvider {
  override def shortName(): String = "geojson"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path",
      throw new IllegalArgumentException("Required parameter 'path' was not supplied. Please provide a valid URI")
    )
    sqlContext.withJTS
    GeoJsonRelation(sqlContext, path)
  }

  case class GeoJsonRelation(sqlContext: SQLContext, path: String)  extends BaseRelation with TableScan {
    override def schema: StructType = {
      val sft = GeoJsonInference.inferSft(path, "geojson")
      sft2StructType(sft)
    }

    override def buildScan(): RDD[Row] = {
      val inferredFields = schema.fields
      val dataTypes = schema.fields.map {_.dataType}

      val fileRdd = sqlContext.sparkContext.wholeTextFiles(path)
      fileRdd.mapPartitions { iter =>
        val geoJsonReader = new GeoJsonReader()
        iter.map { case (_, text) =>
          val parser = new JSONParser(JSONParser.MODE_PERMISSIVE)
          val objects = parser.parse(text).asInstanceOf[JSONArray]
          objects.map { obj =>
            val jsonObject = obj.asInstanceOf[JSONObject]
            // extract and parse geometry object
            val geomObjString = jsonObject.getAsString("geometry")
            val geometry = geoJsonReader.read(geomObjString)
            // same for each of the properties
            val props = jsonObject.get("properties").asInstanceOf[JSONObject]
            val rowValues: mutable.Buffer[Any] = mutable.Buffer.fill[Any](inferredFields.length)(null)
            props.keys.foreach { propName =>
              // find the StructField it belongs to
              val structFieldIndex = inferredFields.indexWhere(sf => propName.equals(sf.name))
              if (structFieldIndex == -1) {
                throw new UnsupportedOperationException("Geojson file does not align with inferred schema")
              }
              val propValue = props.get(propName)
              // Cast type to inferred type
              rowValues(structFieldIndex) = dataTypes(structFieldIndex) match {
                case DataTypes.DoubleType    => if (propValue.getClass.isAssignableFrom(classOf[jl.Double])) {
                  propValue.asInstanceOf[jl.Double]
                } else {
                   propValue match {
                     case i : jl.Integer => i.toDouble
                     case l : jl.Long    => l.toDouble
                     case _ => null // TODO: error or ignore bad records?
                   }
                }
                // TODO: catch other cases where
                case DataTypes.FloatType     => propValue.asInstanceOf[jl.Float]
                case DataTypes.IntegerType   => propValue.asInstanceOf[jl.Integer]
                case DataTypes.StringType    => propValue.asInstanceOf[jl.String]
                case DataTypes.BooleanType   => propValue.asInstanceOf[jl.Boolean]
                case DataTypes.LongType      => propValue.asInstanceOf[jl.Long]
                case DataTypes.TimestampType => propValue.asInstanceOf[java.util.Date]
              }
            }
            Row.fromSeq(rowValues :+ geometry)
          }
        }.flatten
      }
    }

    // NB: this is redundant from GeoMesaSparkSQL
    // Could change scope or could move this class there.
    private def sft2StructType(sft: SimpleFeatureType) = {
      val fields = sft.getAttributeDescriptors.flatMap { ad => ad2field(ad) }.toList
      StructType(fields)
    }

    private def ad2field(ad: AttributeDescriptor): Option[StructField] = {
      import java.{lang => jl}
      val dt = ad.getType.getBinding match {
        case t if t == classOf[jl.Double]                       => DataTypes.DoubleType
        case t if t == classOf[jl.Float]                        => DataTypes.FloatType
        case t if t == classOf[jl.Integer]                      => DataTypes.IntegerType
        case t if t == classOf[jl.String]                       => DataTypes.StringType
        case t if t == classOf[jl.Boolean]                      => DataTypes.BooleanType
        case t if t == classOf[jl.Long]                         => DataTypes.LongType
        case t if t == classOf[java.util.Date]                  => DataTypes.TimestampType

        case t if t == classOf[com.vividsolutions.jts.geom.Point]            => JTSTypes.PointTypeInstance
        case t if t == classOf[com.vividsolutions.jts.geom.MultiPoint]       => JTSTypes.MultiPointTypeInstance
        case t if t == classOf[com.vividsolutions.jts.geom.LineString]       => JTSTypes.LineStringTypeInstance
        case t if t == classOf[com.vividsolutions.jts.geom.MultiLineString]  => JTSTypes.MultiLineStringTypeInstance
        case t if t == classOf[com.vividsolutions.jts.geom.Polygon]          => JTSTypes.PolygonTypeInstance
        case t if t == classOf[com.vividsolutions.jts.geom.MultiPolygon]     => JTSTypes.MultipolygonTypeInstance

        case t if      classOf[Geometry].isAssignableFrom(t)    => JTSTypes.GeometryTypeInstance

        // NB:  List and Map types are not supported.
        case _                                                  => null
      }
      Option(dt).map(StructField(ad.getLocalName, _))
    }
  }

}