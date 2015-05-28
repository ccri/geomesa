package org.locationtech.geomesa.accumulo.data
import org.geotools.data._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

//@RunWith(classOf[JUnitRunner])
class CachingAccumuloDataStoreTest extends AccumuloDataStoreTest {
  override val ds: AccumuloDataStore = DataStoreFinder.getDataStore(Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
    "user"              -> "myuser",
    "password"          -> "mypassword",
    "tableName"         -> defaultTable,
    "useMock"           -> "true",
    "caching"           -> "true",
    "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]
}
