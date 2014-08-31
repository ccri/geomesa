package org.locationtech.geomesa.core.data

import java.util

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{FeatureSource, FeatureStore, DataStoreFinder}
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.filter.FilterUtils._
import org.locationtech.geomesa.core.iterators.TestData
import org.locationtech.geomesa.core.iterators.TestData._
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TableSharingTest extends Specification {

  // Two datastores?

  // Three datasets.  Each with a common field: attr2?

  val sft1 = TestData.getFeatureType("1", tableSharing = true)
  val sft2 = TestData.getFeatureType("2", tableSharing = true)
  val sft3 = TestData.getFeatureType("3", tableSharing = false)

  val tableName = "sharingTest"

  val ds = {
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> tableName,
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]
  }

  // Check existence of tables?
  val mockInstance = new MockInstance("mycloud")
  val c = mockInstance.getConnector("myuser", new PasswordToken("mypassword".getBytes("UTF8")))

  val list: util.SortedSet[String] = c.tableOperations().list
  println(s"Tables: $list")


  // Load up data
  val mediumData1 = mediumData.map(createSF(_, sft1))
  val mediumData2 = mediumData.map(createSF(_, sft2))
  val mediumData3 = mediumData.map(createSF(_, sft3))

  val fs1 = getFeatureStore(ds, sft1, mediumData1)
  val fs2 = getFeatureStore(ds, sft2, mediumData2)
  val fs3 = getFeatureStore(ds, sft3, mediumData3)


  val list2: util.SortedSet[String] = c.tableOperations().list
  println(s"Tables after adding the features: $list2")

  // At least three queries: st, attr, id.

//  val filterStrings = Seq(
//    "IN(100001, 100011)",
//    "attr2 = '2nd100001'",
//    "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
//  )

  def filterCount(f: Filter) = mediumData1.count(f.evaluate)
  def queryCount(f: Filter, fs: SimpleFeatureSource) = fs.getFeatures(f).size

  val id = "IN(100001, 100011)"
  val st = "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
  val at = "attr2 = '2nd100001'"

  // This function compares the number of returned results.
  def compare(fs: String, isFS2deleted: Boolean = false, featureStore2: SimpleFeatureSource = fs2) = {
    val f = ECQL.toFilter(fs)
    val fc = filterCount(f)
    val q1 = queryCount(f, fs1)
    val q2 = queryCount(f, featureStore2)
    val q3 = queryCount(f, fs3)

    s"fc and fs1 get the same results from filter $fs" >> { fc mustEqual q1 }
    s"fs1 and fs3 get the same results from filter $fs" >> { q1 mustEqual q3 }

    if(!isFS2deleted)
      s"fs2 and fs3 get the same results from filter $fs" >> { q2 mustEqual q3 }
    else
      s"fs2 must get 0 from $fs" >> { q2 mustEqual 0 }
  }


  "all three queries" should {
    "work for all three features (after setup) " >> {
      compare(id)
      compare(st)
      compare(at)
    }
  }


  // Delete one shared table feature to ensure that deleteSchema works.
  ds.removeSchema(sft2.getTypeName)

  s"FeatureStore named ${sft2.getTypeName} should be gone" >> { ds.getNames.contains(sft2.getTypeName) must beFalse }

  println(s"Tables: ${c.tableOperations().list")

  // Query again.


  "all three queries" should {
    "work for all three features (after delete) " >> {
      compare(id, true)
      compare(st, true)
      compare(at, true)
    }
  }

  // Reingest a feature or ingest one with the same name.
  val fs2ReIngested = getFeatureStore(ds, sft2, mediumData2)

  // Query again.

  "all three queries" should {
    "work for all three features (after re-ingest) " >> {
      compare(id, featureStore2 = fs2ReIngested)
      compare(st, featureStore2 = fs2ReIngested)
      compare(at, featureStore2 = fs2ReIngested)
    }
  }
}
