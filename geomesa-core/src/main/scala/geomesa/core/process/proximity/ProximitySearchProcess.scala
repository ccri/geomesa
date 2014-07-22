package geomesa.core.process.proximity

import com.vividsolutions.jts.geom.GeometryFactory
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.utils.geotools.Conversions._
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.ReTypingFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.visitor.{CalcResult, FeatureCalc, AbstractCalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Geomesa-enabled Proximity Search",
  description = "Performs a proximity search on a Geomesa feature collection using another feature collection as input"
)
class ProximitySearchProcess {

  private val log = Logger.getLogger(classOf[ProximitySearchProcess])

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "inputFeatures",
                 description = "Input feature collection that defines the proximity search")
               inputFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "dataFeatures",
                 description = "The data set to query for matching features")
               dataFeatures: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "bufferDistance",
                 description = "Buffer size in meters")
               bufferDistance: java.lang.Double

               ): SimpleFeatureCollection = {

    log.info("Attempting Geomesa Proximity Search on collection type " + dataFeatures.getClass.getName)

    if(!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support geomesa proximity search: "+dataFeatures.getClass.getName)
    }
    if(dataFeatures.isInstanceOf[ReTypingFeatureCollection]) {
      log.warn("WARNING: layer name in geoserver must match feature type name in geomesa")
    }

    val visitor = new ProximityVisitor(inputFeatures, dataFeatures, bufferDistance)
    dataFeatures.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[ProximityResult].results
  }
}

class ProximityVisitor( inputFeatures: SimpleFeatureCollection,
                               dataFeatures: SimpleFeatureCollection,
                               bufferDistance: java.lang.Double
                             ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[ProximityVisitor])

  val geoFac = new GeometryFactory
  val ff = CommonFactoryFinder.getFilterFactory2

  var manualFilter: Filter = _
  val manualVisitResults = new DefaultFeatureCollection(null, dataFeatures.getSchema)

  // Called for non AccumuloFeactureCollections - here we use degrees for our filters
  // since we are manually evaluating them.
  def visit(feature: Feature): Unit = {
    manualFilter = Option(manualFilter).getOrElse(dwithinFilters("degrees"))
    val sf = feature.asInstanceOf[SimpleFeature]

    if(manualFilter.evaluate(sf)) {
      manualVisitResults.add(sf)
    }
  }

  var resultCalc: ProximityResult = new ProximityResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = ProximityResult(r)

  def proximitySearch(source: SimpleFeatureSource, query: Query) = {
    log.info("Running Geomesa Proximity Search on source type "+source.getClass.getName)
    val combinedFilter = ff.and(query.getFilter, dwithinFilters("meters"))
    println(s"**** $combinedFilter ****")
    source.getFeatures(combinedFilter)
  }

  def dwithinFilters(requestedUnit: String) = {
    import geomesa.utils.geotools.Conversions.RichGeometry
    import scala.collection.JavaConversions._

    val geomProperty = ff.property(dataFeatures.getSchema.getGeometryDescriptor.getName)
    val geomFilters = inputFeatures.features().map { sf =>
      val dist: Double = requestedUnit match {
        case "degrees" => sf.geometry.distanceDegrees(bufferDistance)
        case _         => bufferDistance
      }
      ff.dwithin(geomProperty, ff.literal(sf.geometry), dist, "meters")
    }
    ff.or(geomFilters.toSeq)
  }
}

case class ProximityResult(results: SimpleFeatureCollection) extends AbstractCalcResult