package geomesa.core.process.proximity

import com.vividsolutions.jts.geom.{GeometryFactory, Geometry}
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.utils.geotools.Conversions._
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.EmptyFeatureCollection
import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.visitor.{CalcResult, FeatureCalc, AbstractCalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.process.vector.VectorProcess
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.geotools.feature.DefaultFeatureCollection
import org.opengis.feature.simple.SimpleFeature

@DescribeProcess(
  title = "Geomesa-enabled Proximity Search",
  description = "Performs a proximity search on a geomesa feature collection using another feature collection as input"
)
class GeomesaProximitySearch extends VectorProcess {

  private val log = Logger.getLogger(classOf[GeomesaProximitySearch])

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

    log.info("Fast Proximity Search on collection type " + dataFeatures.getClass.getName)

    if(!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support fast proximity search: "+dataFeatures.getClass.getName)
    }

    val visitor = new GeomesaProximityVisitor( inputFeatures,
                                            dataFeatures,
                                            bufferDistance)

    dataFeatures.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[GeomesaProximityResult].results
  }
}

class GeomesaProximityVisitor( inputFeatures: SimpleFeatureCollection,
                               dataFeatures: SimpleFeatureCollection,
                               bufferDistance: java.lang.Double
                             ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[GeomesaProximityVisitor])

  val geoFac = new GeometryFactory
  val ff  = CommonFactoryFinder.getFilterFactory2

  val manualVisitResults = new DefaultFeatureCollection(null, dataFeatures.getSchema)

  // Called for non AccumuloFeactureCollections
  def visit(feature: Feature): Unit = {
    val filter = getGeomFilter
    val sf = feature.asInstanceOf[SimpleFeature]
    if(filter.evaluate(sf)) {
      manualVisitResults.add(sf)
    }
  }

  var resultCalc: GeomesaProximityResult = new GeomesaProximityResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = GeomesaProximityResult(r)

  def proximitySearch(source: SimpleFeatureSource, query: Query) = {
    log.info("Proximity searching on source type "+source.getClass.getName)

    val geomOrFilter = getGeomFilter
    val combinedFilter = ff.and(query.getFilter, geomOrFilter)

    source.getFeatures(combinedFilter)
  }

  def getGeomFilter = {
    import scala.collection.JavaConversions._
    val inputGeoms = inputFeatures.features.map { sf =>
      sf.getDefaultGeometry.asInstanceOf[Geometry].bufferMeters(bufferDistance)
    }
    val unionedGeom = geoFac.buildGeometry(inputGeoms.toSeq).union

    val geomProperty = ff.property(dataFeatures.getSchema.getGeometryDescriptor.getName)
    val geomFilters = (0 until unionedGeom.getNumGeometries).map { i =>
      ff.intersects(geomProperty, ff.literal(unionedGeom.getGeometryN(i)))
    }
    ff.or(geomFilters)
  }

}

case class GeomesaProximityResult(results: SimpleFeatureCollection) extends AbstractCalcResult