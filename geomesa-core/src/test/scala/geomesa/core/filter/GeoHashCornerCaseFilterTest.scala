package geomesa.core.filter

import com.typesafe.scalalogging.slf4j.Logging
import geomesa.core.data.{AccumuloDataStoreTest, AccumuloFeatureStore}
import geomesa.core.filter.FilterUtils._
import geomesa.core.iterators.TestData._
import geomesa.utils.geohash.GeoHash
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


object GeoHashCornerCaseFilterTest extends AccumuloDataStoreTest with Logging {
  val ghCornersFeatures: Seq[SimpleFeature] = ghCorners.map(createSF)
  val sft = ghCornersFeatures.head.getFeatureType

  lazy val ds = createStore

  def getFeatureStore: SimpleFeatureSource = {
    val names = ds.getNames

    if (names.size == 0) {
      buildFeatureSource()
    } else {
      ds.getFeatureSource(names(0))
    }
  }

  def buildFeatureSource(): SimpleFeatureSource = {
    ds.createSchema(sft)
    val fs: AccumuloFeatureStore = ds.getFeatureSource(sft.getTypeName).asInstanceOf[AccumuloFeatureStore]
    val coll = new DefaultFeatureCollection(sft.getTypeName)

    val set = ghCornersFeatures.asJavaCollection

    coll.addAll(set)

    logger.debug(s"Adding ${coll.size} ${set.size} SimpleFeatures to feature store.")
    fs.addFeatures(coll)
    logger.debug("Done adding SimpleFeatures to feature store.")

    fs
  }
}

import geomesa.core.filter.GeoHashCornerCaseFilterTest._

trait GeoHashCornerCaseFilterTest extends Specification with Logging {
  lazy val fs = getFeatureStore
  logger.debug(s"Got Feature store $fs")

  def filters: Seq[String]

  def compareFilter(filter: Filter): Fragments = {
    logger.debug(s"Filter: ${ECQL.toCQL(filter)}")

    s"The filter $filter" should {t
      "return the same number of results from filtering and querying" in {
        val filterCount = ghCornersFeatures.count(filter.evaluate)
        val queryCount = fs.getFeatures(filter).size

        logger.debug(s"\nFilter: ${ECQL.toCQL(filter)}\nFullData size: ${ghCornersFeatures.size}: " +
          s"filter hits: $filterCount query hits: $queryCount")
        filterCount mustEqual queryCount
      }
    }
  }

  def runTest = filters.map {s => compareFilter(s) }
}

//@RunWith(classOf[JUnitRunner])
class CornerCaseTest extends Specification with GeoHashCornerCaseFilterTest {

  sequential

  val ghbase32: Seq[Char] = "0123456789bcdefghjkmnpqrstuvwxyz"

  val fiveBitGHPolys = ghbase32.map( c => GeoHash(c.toString).geom.toString )
  val predicates = List("INTERSECTS")
  // val predicates = List("INTERSECTS", "OVERLAPS", "WITHIN", "CONTAINS", "TOUCHES", "EQUALS", "DISJOINT", "CROSSES")

  val allFilters: Seq[String] =
    for {
      p <- predicates
      poly <- fiveBitGHPolys
    } yield s"$p(geom, $poly)"

  //val filters: Seq[String] = Seq("INTERSECTS(geom, POLYGON ((45 0, 45 45, 90 45, 90 0, 45 0)))")
  val filters: Seq[String] = Seq("INTERSECTS(geom, POLYGON ((-180 -90, -180 -45, -135 -45, -135 -90, -180 -90)))")
  //val filters: Seq[String] = Seq("INTERSECTS(geom, POLYGON ((-180 -90, -180 -85, -175 -85, -175 -90, -180 -90)))")  //allFilters.take(1)

  logger.debug("Starting to run test")

  runTest
}
