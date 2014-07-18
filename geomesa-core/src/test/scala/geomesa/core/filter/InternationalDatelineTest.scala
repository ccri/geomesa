package geomesa.core.filter

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.{AccumuloFeatureReader, AccumuloDataStore, AccumuloDataStoreTest, AccumuloFeatureStore}
import geomesa.core.filter.TestFilters._
import geomesa.core.iterators.TestData._
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Fragments
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import geomesa.core.filter.FilterUtils._
import org.geotools.data._
import org.geotools.filter.text.ecql.ECQL


@RunWith(classOf[JUnitRunner])
class InternationalDatelineTest extends IDLFilterTester {
  sequential

  val filters: Seq[String] = Seq(
//    "BBOX(geom, -230.0,-110.0,230.0,110.0)",                                          // 361
    //"BBOX(geom, -100.0,0.0,100.0,1.0)",                                            // 201
//    "BBOX(geom, -100.0,-90.0,100.0,90.0)",                                            // 201
//    "(BBOX(geom, -181.1,-90.0,-175.1,90.0) OR BBOX(geom, 175.1,-90.0,181.1,90.0))",   // 10
    //  "BBOX(geom, -181.1,0.0,40.1,1.0)",
    "BBOX(geom, 175.1,0.0,181.1,1.0)",
//    "(BBOX(geom, -181.1,0.0,40.1,1.0) OR BBOX(geom, 175.1,0.0,181.1,1.0))",      // 226
//  "(BBOX(geom, -181.1,-90.0,40.1,90.0) OR BBOX(geom, 175.1,-90.0,181.1,90.0))"      // 226
  "INTERSECTS(geom, POLYGON ((-100.0 0.0, -100.0 1.0, 100.0 1.0, 100.0 0.0, -100.0 0.0)))"     // SHOULD Wrap the IDL = 160


  )

  runTest
}


object IDLFilterTester extends AccumuloDataStoreTest with Logging {
  val allThePointsFeatures: Seq[SimpleFeature] = allThePoints.map(createSF)
  val sft = allThePointsFeatures.head.getFeatureType

  logger.debug("In IDLFilterTester building the data store.")
  val ds = createStore

  def getFeatureStore: SimpleFeatureSource = {
    val names = ds.getNames

    if(names.size == 0) {
      buildFeatureSource()
    } else {
      ds.getFeatureSource(names(0))
    }
  }

  def buildFeatureSource(): SimpleFeatureSource = {
    ds.createSchema(sft)
    val fs: AccumuloFeatureStore = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]
    val coll = new DefaultFeatureCollection(sft.getTypeName)
    coll.addAll(allThePointsFeatures.asJavaCollection)

    logger.debug("Adding SimpleFeatures to feature store.")
    fs.addFeatures(coll)
    logger.debug("Done adding SimpleFeaturest to feature store.")

    fs
  }

}

import geomesa.core.filter.IDLFilterTester._

trait IDLFilterTester extends Specification with Logging {
  lazy val fs = getFeatureStore
  lazy val ds = fs.getDataStore.asInstanceOf[AccumuloDataStore]

  val q = new Query()
  q.setTypeName(ds.getNames.apply(0).toString)
  val trans = Transaction.AUTO_COMMIT

  val afr = ds.getFeatureReader(q, trans).asInstanceOf[AccumuloFeatureReader]
  val indexSchema = afr.indexSchema

  def filters: Seq[String]

  def log(s: String) = logger.debug(s)

  def compareFilter(filter: Filter): Fragments = {
    logger.debug(s"Filter: ${ECQL.toCQL(filter)}")

    s"The filter $filter" should {
      "return the same number of results from filtering and querying" in {

        fs.getDataStore

//        logger.debug(s"Setting filter: $filter")
//        q.setFilter(filter)
//        logger.debug(s"Query is $q")
//        indexSchema.explainQuery(q, log)
//
//        logger.debug(s"Done explaining: Querying now.")

        val filteredPoints = allThePointsFeatures.filter(filter.evaluate)
        logger.debug("Filtered")
        filteredPoints.foreach{f => println(f.getDefaultGeometry)}

        val filterCount = filteredPoints.size // allThePointsFeatures.count(filter.evaluate)

        val qp = fs.getFeatures(filter)
          val queryPoints = qp.features
        logger.debug("queryPoints")
        while (queryPoints.hasNext) {
          println(queryPoints.next.getDefaultGeometry)
        }
        //queryPoints.toArray.foreach{println}

        val queryCount = qp.size

        logger.debug(s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${allThePointsFeatures.size}: " +
          s"filter hits: $filterCount query hits: $queryCount")
        filterCount mustEqual queryCount
      }
    }
  }

  def runTest = filters.map {s => compareFilter(s) }
}