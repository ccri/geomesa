package org.locationtech.geomesa.feature

import java.io._
import java.text.SimpleDateFormat
import java.util.UUID

import com.vividsolutions.jts.geom.{LineString, Point, Polygon}
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.avro.io.DecoderFactory
import org.geotools.data.DataUtilities
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.{Assert, Test}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

class FeatureSpecificReaderTest {


  def createTypeWithGeo: AvroSimpleFeature = {
    val sft = SimpleFeatureTypes.createType("test","f0:Point,f1:Polygon,f2:LineString")
    val sf = new AvroSimpleFeature(new FeatureIdImpl("fakeid"), sft)

    sf.setAttribute("f0", GeohashUtils.wkt2geom("POINT(45.0 49.0)").asInstanceOf[Point])
    sf.setAttribute("f1", GeohashUtils.wkt2geom("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))").asInstanceOf[Polygon])
    sf.setAttribute("f2", GeohashUtils.wkt2geom("LINESTRING(47.28515625 25.576171875, 48 26, 49 27)").asInstanceOf[LineString])

    sf
  }

  def writeAvroFile(sfList: List[AvroSimpleFeature]) : File = {
    val f = File.createTempFile("avro", ".tmp")
    f.deleteOnExit()
    val fos = new FileOutputStream(f)
    sfList.foreach( sf => sf.write(fos))
    fos.close()
    f
  }

  def writePipeFile(sfList: List[AvroSimpleFeature]) : File = {
    val f = File.createTempFile("pipe", ".tmp")
    f.deleteOnExit()
    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f)))
    sfList.foreach { f =>
      writer.write(DataUtilities.encodeFeature(f, true))
      writer.newLine()
    }
    writer.close()
    f
  }

  def readAvroWithFsr(f: File, oldType: SimpleFeatureType): List[AvroSimpleFeature] = readAvroWithFsr(new FeatureSpecificReader(oldType, oldType), f)

  def readAvroWithFsr(fsr: FeatureSpecificReader, f: File): List[AvroSimpleFeature] = {
    val fis = new FileInputStream(f)
    val decoder = DecoderFactory.get().binaryDecoder(fis, null)

    val sfList = new ListBuffer[AvroSimpleFeature]()

    do {
      sfList += fsr.read(null, decoder)
    } while(!decoder.isEnd)

    fis.close()
    sfList.toList
  }

  def readPipeFile(f:File, sft:SimpleFeatureType) : List[SimpleFeature] = {
    val sfList = ListBuffer[SimpleFeature]()
    for (line <- Source.fromFile(f).getLines()){
      sfList += DataUtilities.createFeature(sft, line)
    }
    sfList.toList
  }


  def randomString(fieldId: Int, len: Int, r:Random) : String = {
    val sb = new mutable.StringBuilder()
    for(i <- 0 until len) {
      sb.append(fieldId)
    }
    sb.toString()
  }

  def createStringFeatures(schema:String, size: Int, id: String) : AvroSimpleFeature = {
    val sft = SimpleFeatureTypes.createType("test", schema)
    val r = new Random()
    r.setSeed(0)

    var lst = new mutable.MutableList[String]
    for(i <- 0 until size)
      lst += randomString(i, 8, r)

    val sf = new AvroSimpleFeature(new FeatureIdImpl(id), sft)
    for(i <- 0 until lst.size) {
      sf.setAttribute(i, lst(i))
    }
    sf
  }


  def getSubsetData : List[AvroSimpleFeature] = {
    val numFields = 60
    val numRecords = 10

    val sb = new mutable.StringBuilder()
    for(i <- 0 until numFields) {
      if(i != 0) sb.append(",")
      sb.append(f"f$i%d:String")
    }
    val geoSchema = sb.toString()

    val sfList: List[AvroSimpleFeature] =
      for (i <- (0 until numRecords).toList) yield createStringFeatures(geoSchema, numFields,i.toString)

    val oldType = sfList(0).getType

    val f = writeAvroFile(sfList)
    val subsetType = SimpleFeatureTypes.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String")
    val subsetList = readAvroWithFsr(new FeatureSpecificReader(oldType, subsetType), f)

    subsetList
  }

  @Test
  def testSubset() = {
    val subset = getSubsetData
    Assert.assertEquals(10, subset.size)

    subset.foreach(sf => {
      Assert.assertEquals(5, sf.getAttributeCount)
      Assert.assertEquals(5, sf.getAttributes.size())
      import scala.collection.JavaConversions._
      sf.getAttributes.foreach(attr => {
        Assert.assertTrue(classOf[String].isAssignableFrom(attr.getClass))
        Assert.assertNotNull(attr.asInstanceOf[String])
      })

      Assert.assertEquals("00000000", sf.getAttribute("f0"))
      Assert.assertEquals("11111111", sf.getAttribute("f1"))
      Assert.assertEquals("33333333", sf.getAttribute("f3"))
      Assert.assertEquals("3030303030303030", sf.getAttribute("f30"))
      Assert.assertEquals("5959595959595959", sf.getAttribute("f59"))
    })
  }

  @Test
  def testMemberNotInSubsetIsNull(): Unit = {
    Assert.assertNull(getSubsetData(0).getAttribute("f20"))
  }


  @Test
  def testGeoTypes() = {
    val orig = createTypeWithGeo
    val f = writeAvroFile(List(orig))
    val fsrList = readAvroWithFsr(new FeatureSpecificReader(orig.getType, orig.getType), f)

    Assert.assertEquals(1, fsrList.size)
    val sf = fsrList(0)
    Assert.assertEquals(3, sf.getAttributeCount)
    Assert.assertEquals(orig.getAttributeCount, sf.getAttributeCount)

    Assert.assertEquals(orig.getAttribute("f0"), sf.getAttribute("f0"))
    Assert.assertEquals(orig.getAttribute("f1"), sf.getAttribute("f1"))
    Assert.assertEquals(orig.getAttribute("f2"), sf.getAttribute("f2"))
  }

  def buildStringSchema(numFields: Int) : String = {
    val sb = new StringBuilder()
    for (i <- 0 until numFields) {
      if (i != 0) sb.append(",")
      sb.append(f"f$i%d:String")
    }
    sb.toString()
  }

  @Test
  def testSimpleDeserialize() = {
    val numFields = 60
    val numRecords = 100
    val geoSchema = buildStringSchema(numFields)

    val sfList :List[AvroSimpleFeature] =
      for (i <- (0 until numRecords).toList) yield  createStringFeatures(geoSchema, numFields, i.toString)

    val oldType = sfList(0).getType
    val avroFile = writeAvroFile(sfList)
    val pipeFile = writePipeFile(sfList)

    val subsetType = SimpleFeatureTypes.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String")
    val fsrList = readAvroWithFsr(new FeatureSpecificReader(oldType, subsetType), avroFile)
    val pipeList = readPipeFile(pipeFile, oldType)

    Assert.assertEquals(sfList.size, pipeList.size)
    Assert.assertEquals(fsrList.size, pipeList.size)

    for(i <- 0 until sfList.size) {
      val f1 = sfList(i).asInstanceOf[SimpleFeature]
      val f2 = fsrList(i).asInstanceOf[SimpleFeature]
      val f3 = pipeList(i)

      Assert.assertEquals(f1.getID, f2.getID)
      Assert.assertEquals(f1.getID, f3.getID)

      Assert.assertEquals(numFields, f1.getAttributeCount)
      Assert.assertEquals(5, f2.getAttributeCount)     //subset
      Assert.assertEquals(numFields, f3.getAttributeCount)

      for(s <- List("f0","f1", "f3", "f30", "f59")) {
        Assert.assertEquals(f1.getAttribute(s), f2.getAttribute(s))
        Assert.assertEquals(f2.getAttribute(s), f3.getAttribute(s))
      }

      Assert.assertFalse(f1.equals(f2))
    }
  }

  def createComplicatedFeatures(numFeatures : Int) : List[AvroSimpleFeature] = {
    val geoSchema = "f0:String,f1:Integer,f2:Double,f3:Float,f4:Boolean,f5:UUID,f6:Date,f7:Point:srid=4326,f8:Polygon:srid=4326"
    val sft = SimpleFeatureTypes.createType("test", geoSchema)
    val r = new Random()
    r.setSeed(0)


    val list = new ListBuffer[AvroSimpleFeature]
    for(i <- 0 until numFeatures){
      val fid = new FeatureIdImpl(r.nextString(5))
      val sf = new AvroSimpleFeature(fid, sft)

      sf.setAttribute("f0", r.nextString(10).asInstanceOf[Object])
      sf.setAttribute("f1", r.nextInt().asInstanceOf[Object])
      sf.setAttribute("f2", r.nextDouble().asInstanceOf[Object])
      sf.setAttribute("f3", r.nextFloat().asInstanceOf[Object])
      sf.setAttribute("f4", r.nextBoolean().asInstanceOf[Object])
      sf.setAttribute("f5", UUID.fromString("12345678-1234-1234-1234-123456789012"))
      sf.setAttribute("f6", new SimpleDateFormat("yyyyMMdd").parse("20140102"))
      sf.setAttribute("f7", GeohashUtils.wkt2geom("POINT(45.0 49.0)").asInstanceOf[Point])
      sf.setAttribute("f8", GeohashUtils.wkt2geom("POLYGON((-80 30,-80 23,-70 30,-70 40,-80 40,-80 30))").asInstanceOf[Polygon])
      list += sf
    }
    list.toList
  }

  @Test
  def testComplexDeserialize() = {
    val numRecords = 1
    val sfList = createComplicatedFeatures(numRecords)
    val oldType = sfList(0).getType

    val avroFile = writeAvroFile(sfList)
    val pipeFile = writePipeFile(sfList)

    val subsetType = SimpleFeatureTypes.createType("subsetType", "f0:String,f3:Float,f5:UUID,f6:Date")

    val pipeList = readPipeFile(pipeFile, oldType)

    val avroList = readAvroWithFsr(new FeatureSpecificReader(oldType, subsetType), avroFile)

    Assert.assertEquals(pipeList.size, avroList.size)
    Assert.assertEquals(numRecords, avroList.size)

    for(i <- 0 until numRecords){
      val a = pipeList(i)
      val b = avroList(i)

      for(s <- List("f0","f3", "f5", "f6")) {
        Assert.assertEquals(a.getAttribute(s), b.getAttribute(s))
        Assert.assertEquals(a.getAttribute(s), sfList(i).getAttribute(s))
      }

    }
  }

  @Test
  def speedTestWithStringFields() = {
    println("Beginning Performance Testing against file...")
    val numFields = 60
    val numRecords = 1000
    println(f"Number of fields: $numFields%d")
    println(f"Number of records: $numRecords%d")

    val geoSchema = buildStringSchema(numFields)
    val sfList = for (i <- (0 until numRecords).toList) yield  createStringFeatures(geoSchema, numFields, i.toString)

    val oldType = sfList(0).getType
    val subsetType = SimpleFeatureTypes.createType("subsetType", "f0:String,f1:String,f3:String,f30:String,f59:String")

    val avroFile = writeAvroFile(sfList)
    val pipeFile = writePipeFile(sfList)

    val pipeStart = System.currentTimeMillis()
    readPipeFile(pipeFile, oldType)
    val pipeTime = System.currentTimeMillis() - pipeStart
    println(f"Text Read time $pipeTime%dms")

    val fsr = new FeatureSpecificReader(oldType, subsetType)
    val avroStart = System.currentTimeMillis()
    readAvroWithFsr(fsr, avroFile)
    val avroTime = System.currentTimeMillis() - avroStart
    println(f"Avro Subset Read time $avroTime%dms")

  }

}
