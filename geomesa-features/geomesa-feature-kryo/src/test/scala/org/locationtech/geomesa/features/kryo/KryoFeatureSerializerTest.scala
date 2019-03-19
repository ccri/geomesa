/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.features.kryo

import java.io.{BufferedInputStream, ByteArrayInputStream, FileInputStream, ObjectInputStream}
import java.nio.charset.StandardCharsets
import java.util
import java.util.{Date, Scanner, UUID}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.codec.binary.Base64
import org.json4s.FileInput
import org.junit.runner.RunWith
import org.locationtech.geomesa.features.AbstractSimpleFeature.AbstractImmutableSimpleFeature
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SerializationOption}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.AttributeOptions
import org.locationtech.geomesa.utils.geotools.{ImmutableFeatureId, SimpleFeatureTypes}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuilder

@RunWith(classOf[JUnitRunner])
class KryoFeatureSerializerTest extends Specification with LazyLogging {

  import SerializationOption._

  sequential

  val options = Seq(
      Set.empty[SerializationOption],
      Set(Immutable),
      Set(WithUserData),
      Set(Lazy),
      Set(Immutable, WithUserData),
      Set(Lazy, Immutable),
      Set(Lazy, WithUserData),
      Set(Lazy, Immutable, WithUserData)
    )

  "KryoFeatureSerializer" should {

    def arrayEquals(a: Any, b: Any): MatchResult[Boolean] = {
      val aBytes = a.asInstanceOf[Array[Byte]]
      val bBytes = b.asInstanceOf[Array[Byte]]
      util.Arrays.equals(aBytes, bBytes) must beTrue
    }

    "do good things" in {
      val spec = "registrationid:String:index=full,ts:Long,source:String,app_name:String,environment:String,geo_country:String,inventory_source:String,platform_browser:String,platform_device_make:String,platform_os:String,platform_os_version:String,placement_typ:String,metadata:String:json=true,dtg:Date:default=true,*geom:Point:srid=4326:precision=6"
      val sft = SimpleFeatureTypes.createType("rtb", spec)

      val sf = new ScalaSimpleFeature(sft, "fakeid")

      val serializer = KryoFeatureSerializer(sft, SerializationOptions.none)

      def deserializeInt(bytes: Array[Byte]): Int = {
        val stream = new ByteArrayInputStream(bytes)
        val ois = new ObjectInputStream(stream)
        try {
          ois.readInt()
        } finally {
          ois.close()
          stream.close()
        }
      }

      def readInt(bis: BufferedInputStream): Int = {
        val intArr = new Array[Byte](10)
        val readLength = bis.read(intArr, 0, 10)
        require(readLength == 10, "Invalid file")
        deserializeInt(intArr)
      }

      def readArray(bis: BufferedInputStream): Array[Byte] = {
        val length = readInt(bis)
        val keyArr = new Array[Byte](length)
        bis.read(keyArr, 0, length)
        keyArr
      }

      val is = getClass.getClassLoader.getResourceAsStream(s"bad_data")
      val bis = new BufferedInputStream(is)
      val value = readArray(bis)
      val valueOffset = readInt(bis)
      val valueLength = readInt(bis)

      val deserialized = KryoFeatureSerializer(sft, Set.empty[SerializationOption]).deserialize(value, valueOffset, valueLength)

      //      val badValueRaw = "\\x02\\x00\\x00\\x00\\xE6\\xC1\\x016b88c933c27c7f744b955afdf7dbf7d0f3810de8143c5aa59cf3fdaea46b0653\\x01\\x00\\x05~Z\\xC7\\x1F\\xBAx\\x82B\\xC1\\x011f890c5900dfba03c7b0d0c34fc9267ec0b6f51dc94902a14a3bf745e1f08adeAP\\xD0KORGOOGLE_AD\\xD8Chrome Mobil\\xE5Samsun\\xE7Androi\\xE48.\\xB0BANNER_AND_VIDE\\xCF\\x00\\x01\\x00\\x00\\x01h\\x06\\xDA\\xE9g\\xC1\\x00\\xBC\\x95\\xFEx\\xA8\\xED\\xBE!\\x05GPR\\x94\\x01\\x97\\x01\\x9A\\x01\\xA4\\x01\\xB1\\x01\\xB8\\x01\\xBF\\x01\\xC2\\x01\\xD2\\x01\\xD3\\x01\\xDC\\x01"

      //      foreach(options) { opts =>
      //      val deserialized = KryoFeatureSerializer(sft, Set.empty[SerializationOption]).deserialize(badValueRaw.toCharArray.map(_.toByte))
      //      }


      deserialized must not(beNull)
      deserialized.getType mustEqual sf.getType
    }

    "correctly deserialize basic features" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326,bytes:Bytes"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      val userData = Map("key.one" -> java.lang.Boolean.TRUE, "key.two" -> "value.two")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")
      sf.setAttribute("bytes", "\u0000FOOBARBAZ\u0000\u4444123".getBytes(StandardCharsets.UTF_16BE))
      sf.getUserData.putAll(userData)

      forall(options) { opts =>
        val serializer = KryoFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized.getID mustEqual sf.getID
        deserialized.getAttributes.dropRight(1) mustEqual sf.getAttributes.dropRight(1)
        arrayEquals(sf.getAttributes.last, "\u0000FOOBARBAZ\u0000\u4444123".getBytes(StandardCharsets.UTF_16BE))
        arrayEquals(deserialized.getAttributes.last, sf.getAttributes.last)

        if (opts.withUserData) {
          deserialized.getUserData.toMap mustEqual userData
        } else {
          deserialized.getUserData must beEmpty
        }

        if (opts.immutable) {
          deserialized must beAnInstanceOf[AbstractImmutableSimpleFeature]
          deserialized.getIdentifier must beAnInstanceOf[ImmutableFeatureId]
          deserialized.setAttribute(0, 2) must throwAn[UnsupportedOperationException]
          deserialized.setAttribute("a", 2) must throwAn[UnsupportedOperationException]
          deserialized.setAttributes(Array.empty[AnyRef]) must throwAn[UnsupportedOperationException]
          deserialized.setAttributes(Seq.empty[AnyRef]) must throwAn[UnsupportedOperationException]
          deserialized.getUserData.put("foo", "bar") must throwAn[UnsupportedOperationException]
        } else {
          deserialized.getUserData.put("foo", "bar")
          deserialized.getUserData.get("foo") mustEqual "bar"
        }
      }
    }

    "correctly serialize and deserialize different geometries" in {
      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
        "f:GeometryCollection,dtg:Date,*geom:Point:srid=4326"
      val sftWkb = SimpleFeatureTypes.createType("testTypeWkb", spec)
      // use a different name to avoid cached serializers
      val sftTwkb = SimpleFeatureTypes.createType("testTypeTwkb", spec)
      sftTwkb.getAttributeDescriptors.foreach(_.getUserData.put(AttributeOptions.OPT_PRECISION, "6"))

      val sf = new ScalaSimpleFeature(sftWkb, "fakeid")
      sf.setAttribute("a", "LINESTRING(0 2, 2 0, 8 6)")
      sf.setAttribute("b", "POLYGON((20 10, 30 0, 40 10, 30 20, 20 10))")
      sf.setAttribute("c", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("d", "MULTILINESTRING((0 2, 2 0, 8 6),(0 2, 2 0, 8 6))")
      sf.setAttribute("e", "MULTIPOLYGON(((-1 0, 0 1, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), " +
          "((-1 5, 2 5, 2 2, -1 2, -1 5)))")
      sf.setAttribute("f", "MULTIPOINT(0 0, 2 2)")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(55.0 49.0)")

      forall(Seq(sftWkb, sftTwkb)) { sft =>
        forall(options) { opts =>
          val serializer = KryoFeatureSerializer(sft, opts)
          val serialized = serializer.serialize(sf)
          val deserialized = serializer.deserialize(serialized)

          deserialized must not(beNull)
          deserialized.getType mustEqual sft
          deserialized.getAttributes mustEqual sf.getAttributes
        }
      }
    }

    "correctly serialize and deserialize geometries with n dimensions" in {
      val spec = "a:LineString,b:Polygon,c:MultiPoint,d:MultiLineString,e:MultiPolygon," +
          "f:GeometryCollection,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "LINESTRING(0 2 0, 2 0 1, 8 6 2)")
      sf.setAttribute("b", "POLYGON((20 10 0, 30 0 10, 40 10 10, 30 20 0, 20 10 0))")
      sf.setAttribute("c", "MULTIPOINT(0 0 0, 2 2 2)")
      sf.setAttribute("d", "MULTILINESTRING((0 2 0, 2 0 1, 8 6 2),(0 2 0, 2 0 0, 8 6 0))")
      sf.setAttribute("e", "MULTIPOLYGON(((-1 0 0, 0 1 0, 1 0 0, 0 -1 0, -1 0 0)), ((-2 6 2, 1 6 3, 1 3 3, -2 3 3, -2 6 2)), " +
          "((-1 5 0, 2 5 0, 2 2 0, -1 2 0, -1 5 0)))")
      sf.setAttribute("f", "MULTIPOINT(0 0 2, 2 2 0)")
      sf.setAttribute("geom", "POINT(55.0 49.0 37.0)")

      forall(options) { opts =>
        val serializer = KryoFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
        forall(deserialized.getAttributes.zip(sf.getAttributes)) { case (left, right) =>
          forall(left.asInstanceOf[Geometry].getCoordinates.zip(right.asInstanceOf[Geometry].getCoordinates)) {
            case (c1, c2) => c1.equals3D(c2) must beTrue
          }
        }
      }
    }

    "correctly serialize and deserialize collection types" in {
      val spec = "a:Integer,m:Map[String,Double],l:List[Date],dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("m", Map("test1" -> 1.0, "test2" -> 2.0))
      sf.setAttribute("l", List(new Date(100), new Date(200)))
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      forall(options) { opts =>
        val serializer = KryoFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }

    "serialize maps and lists of bytes" >> {
      val spec = "m1:Map[String,Bytes],l:List[Bytes],dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("m1", Map("a" -> Array(0.toByte, 23.toByte)))
      sf.setAttribute("l", List[Array[Byte]](Array(0.toByte, 23.toByte), Array(1.toByte)))
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      forall(options) { opts =>
        val serializer = KryoFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        import org.locationtech.geomesa.utils.geotools.Conversions._
        arrayEquals(deserialized.get[java.util.Map[String,_]]("m1")("a"), sf.get[java.util.Map[String,_]]("m1")("a"))
        arrayEquals(deserialized.get[java.util.List[_]]("l")(0), sf.get[java.util.List[_]]("l")(0))
      }
    }

    "correctly serialize and deserialize null values" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,l:List,m:Map," +
        "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      forall(options) { opts =>
        val serializer = KryoFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val deserialized = serializer.deserialize(serialized)

        deserialized must not(beNull)
        deserialized.getType mustEqual sf.getType
        deserialized.getAttributes.foreach(_ must beNull)
        deserialized.getAttributes mustEqual sf.getAttributes
      }
    }

    "correctly serialize and deserialize sub-arrays" in {
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)
      val sf = new ScalaSimpleFeature(sft, "fakeid")

      val userData = Map("key.one" -> java.lang.Boolean.TRUE, "key.two" -> "value.two")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")
      sf.getUserData.putAll(userData)

      forall(options) { opts =>
        val serializer = KryoFeatureSerializer(sft, opts)
        val serialized = serializer.serialize(sf)
        val extra = Array.fill[Byte](128)(-1)
        val bytes = Seq((serialized ++ extra, 0), (extra ++ serialized, extra.length), (extra ++ serialized ++ extra, extra.length))

        forall(bytes) { case (array, offset) =>
          val deserialized = serializer.deserialize(array, offset, serialized.length)
          deserialized must not(beNull)
          deserialized.getType mustEqual sf.getType
          deserialized.getAttributes mustEqual sf.getAttributes

          if (opts.withUserData) {
            deserialized.getUserData.toMap mustEqual userData
          } else {
            deserialized.getUserData must beEmpty
          }

          if (opts.immutable) {
            deserialized must beAnInstanceOf[AbstractImmutableSimpleFeature]
            deserialized.getIdentifier must beAnInstanceOf[ImmutableFeatureId]
            deserialized.setAttribute(0, 2) must throwAn[UnsupportedOperationException]
            deserialized.setAttribute("a", 2) must throwAn[UnsupportedOperationException]
            deserialized.setAttributes(Array.empty[AnyRef]) must throwAn[UnsupportedOperationException]
            deserialized.setAttributes(Seq.empty[AnyRef]) must throwAn[UnsupportedOperationException]
            deserialized.getUserData.put("foo", "bar") must throwAn[UnsupportedOperationException]
          } else {
            deserialized.getUserData.put("foo", "bar")
            deserialized.getUserData.get("foo") mustEqual "bar"
          }
        }
      }
    }

    "correctly project features" in {
      val sft = SimpleFeatureTypes.createType("fullType", "name:String,*geom:Point,dtg:Date")
      val projectedSft = SimpleFeatureTypes.createType("projectedType", "*geom:Point")

      val sf = new ScalaSimpleFeature(sft, "testFeature")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = KryoFeatureSerializer(sft)
      val deserializer = new ProjectingKryoFeatureDeserializer(sft, projectedSft)

      val serialized = serializer.serialize(sf)
      val deserialized = deserializer.deserialize(serialized)

      deserialized.getID mustEqual sf.getID
      deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
      deserialized.getAttributeCount mustEqual 1
    }

    "correctly project features to larger sfts" in {
      val sft = SimpleFeatureTypes.createType("fullType", "name:String,*geom:Point,dtg:Date")
      val projectedSft = SimpleFeatureTypes.createType("projectedType",
        "name1:String,name2:String,*geom:Point,otherDate:Date")

      val sf = new ScalaSimpleFeature(sft, "testFeature")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = KryoFeatureSerializer(sft)
      val deserializer = new ProjectingKryoFeatureDeserializer(sft, projectedSft)

      val serialized = serializer.serialize(sf)
      val deserialized = deserializer.deserialize(serialized)

      deserialized.getID mustEqual sf.getID
      deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
      deserialized.getAttributeCount mustEqual 4
    }

    "allow for attributes to be appended to the sft" in {
      val sft = SimpleFeatureTypes.createType("mutableType", "name:String,*geom:Point,dtg:Date")

      val sf = new ScalaSimpleFeature(sft, "testFeature")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val newSft = SimpleFeatureTypes.createType("mutableType", "name:String,*geom:Point,dtg:Date,attr1:String,attr2:Long")

      // note: can't append attributes while also serializing user data
      forall(options.filterNot(_.withUserData)) { opts =>
        val serialized: Array[Byte] = KryoFeatureSerializer(sft, opts).serialize(sf)
        val deserialized = KryoFeatureSerializer(newSft, opts).deserialize(serialized)

        deserialized.getID mustEqual sf.getID
        deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
        deserialized.getAttributeCount mustEqual 5
        deserialized.getAttribute(3) must beNull
        deserialized.getAttribute(4) must beNull
        deserialized.getAttribute("attr1") must beNull
        deserialized.getAttribute("attr2") must beNull
      }
    }

    "allow for attributes to be appended to the sft and still transform" in {
      val sft = SimpleFeatureTypes.createType("mutableType", "name:String,*geom:Point,dtg:Date")

      val sf = new ScalaSimpleFeature(sft, "testFeature")
      sf.setAttribute("name", "foo")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serialized = KryoFeatureSerializer(sft).serialize(sf)

      val newSft = SimpleFeatureTypes.createType("mutableType", "name:String,*geom:Point,dtg:Date,attr1:String,attr2:Long")
      val projectedSft = SimpleFeatureTypes.createType("projectedType", "*geom:Point")

      val deserialized = new ProjectingKryoFeatureDeserializer(newSft, projectedSft).deserialize(serialized)

      deserialized.getID mustEqual sf.getID
      deserialized.getDefaultGeometry mustEqual sf.getDefaultGeometry
      deserialized.getAttributeCount mustEqual 1
    }

    "handle corrupt data by returning nulls" in {
      val sft = SimpleFeatureTypes.createType("corruptType", "age:Int,dtg:Date,*geom:Point")

      val sf = new ScalaSimpleFeature(sft, "testFeature")
      sf.setAttribute("age", "10")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")
      sf.getUserData.put("foo", "bar")

      foreach(options) { opts =>
        val serialized = KryoFeatureSerializer(sft, opts).serialize(sf)
        // mess up the bytes for 'geom' - this change was picked semi-randomly but works to fail the deserializer
        serialized(32) = Byte.MaxValue

        val deserialized = KryoFeatureSerializer(sft, opts).deserialize(serialized)

        deserialized.getID mustEqual sf.getID
        deserialized.getAttributeCount mustEqual 3
        deserialized.getAttribute(0) mustEqual sf.getAttribute(0)
        deserialized.getAttribute(1) mustEqual sf.getAttribute(1)
        deserialized.getAttribute(2) must beNull
      }
    }

    "be backwards compatible" in {
      val spec = "dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("testType", spec)

      val sf = new ScalaSimpleFeature(sft, "fakeid")
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = KryoFeatureSerializer(sft)
      // base64 encoded bytes from version 1 of the kryo feature serializer
      val version0SerializedBase64 = "AWZha2Vp5AEAAAE7+I60AAEBQEaAAAAAAABASIAAAAAAAA=="
      val version1Bytes = Base64.decodeBase64(version0SerializedBase64)

      val deserialized = serializer.deserialize(version1Bytes)

      deserialized must not(beNull)
      deserialized.getType mustEqual sf.getType
      deserialized.getAttributes mustEqual sf.getAttributes
    }.pendingUntilFixed("dropping back compatibility")

    "be faster than full deserialization" in {
      skipped("integration")
      val spec = "a:Integer,b:Float,c:Double,d:Long,e:UUID,f:String,g:Boolean,dtg:Date,*geom:Point:srid=4326"
      val sft = SimpleFeatureTypes.createType("speed", spec)

      val sf = new ScalaSimpleFeature(sft, "fakeid")

      sf.setAttribute("a", "1")
      sf.setAttribute("b", "1.0")
      sf.setAttribute("c", "5.37")
      sf.setAttribute("d", "-100")
      sf.setAttribute("e", UUID.randomUUID())
      sf.setAttribute("f", "mystring")
      sf.setAttribute("g", java.lang.Boolean.FALSE)
      sf.setAttribute("dtg", "2013-01-02T00:00:00.000Z")
      sf.setAttribute("geom", "POINT(45.0 49.0)")

      val serializer = KryoFeatureSerializer(sft, SerializationOptions.none)
      val serialized = serializer.serialize(sf)

      val start = System.currentTimeMillis()
      (0 until 1000000).foreach { _ =>
        val de = serializer.deserialize(serialized)
        de.getAttribute(1)
      }
      logger.debug(s"took ${System.currentTimeMillis() - start}ms")

      val start2 = System.currentTimeMillis()
      val reusable = serializer.getReusableFeature
      (0 until 1000000).foreach { _ =>
        reusable.setBuffer(serialized)
        reusable.getAttribute(7)
      }
      logger.debug(s"took ${System.currentTimeMillis() - start2}ms")

      logger.debug("\n\n")
      success
    }


  }
}
