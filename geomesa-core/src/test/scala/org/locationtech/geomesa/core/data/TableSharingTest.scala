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
  sequential

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
  def filterCount(f: Filter) = mediumData1.count(f.evaluate)
  def queryCount(f: Filter, fs: SimpleFeatureSource) = fs.getFeatures(f).size

  val id = "IN(100001, 100011)"
  val st = "INTERSECTS(geom, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))"
  val at = "attr2 = '2nd100001'"

  // This function compares the number of returned results.
  def compare(fs: String, step: Int, featureStore2: SimpleFeatureSource = fs2) = {
    val f = ECQL.toFilter(fs)
    val fc = filterCount(f)
    val q1 = queryCount(f, fs1)

    val q3 = queryCount(f, fs3)

    println(s"Compare (step: $step filter: $f fc: $fc q1: $q1 q3: $q3")

    step match {
      case 1 => check(q3)
      case 2 =>
      case 3 => check(0)   // Feature source #2 should be empty
      case 4 => check(q3)
    }

    def check(count: Int) = {

      val q2 = queryCount(f, featureStore2)
      println(s"Q2: $q2 for step $step for filter $fs")

      s"fs2 must get $count results from filter $fs" >> {
        q2 mustEqual count
      }
    }

    s"fc and fs1 get the same results from filter $fs" >> { fc mustEqual q1 }
    s"fs1 and fs3 get the same results from filter $fs" >> { q1 mustEqual q3 }
  }

  "all three queries" should {
    "work for all three features (after setup) " >> {
      compare(id, 1)
      compare(st, 1)
      compare(at, 1)
    }
  }

  // Delete one shared table feature to ensure that deleteSchema works.
  s"Removing ${sft2.getTypeName}" should {
    ds.removeSchema(sft2.getTypeName)

    println(s"Tables (after delete): ${c.tableOperations().list}")

    println(s"GetNames after delete ${ds.getNames}")

    s"result in FeatureStore named ${sft2.getTypeName} being gone" >> {
      ds.getNames.contains(sft2.getTypeName) must beFalse
    }
  }

  // Query again.
  "all three queries" should {
    "work for all three features (after delete) " >> {
      compare(id, 2)
      compare(st, 2)
      compare(at, 2)
    }
  }

  // Query again after recreating just the SFT for feature source 2.
  "all three queries" should {
    ds.createSchema(sft2)

    "work for all three features (after recreating the schema for SFT2) " >> {
      compare(id, 3)
      compare(st, 3)
      compare(at, 3)
    }
  }

  // Query again.
  "all three queries" should {
    // Reingest a feature or ingest one with the same name.
    val fs2ReIngested = getFeatureStore(ds, sft2, mediumData2)

    "work for all three features (after re-ingest) " >> {
      compare(id, 4, featureStore2 = fs2ReIngested)
      compare(st, 4, featureStore2 = fs2ReIngested)
      compare(at, 4, featureStore2 = fs2ReIngested)
    }
  }
}
