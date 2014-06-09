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
import org.opengis.filter.Filter

@DescribeProcess(
  title = "Fast Proximity Search on Feature Collections",
  description = "Performs a proximity search on a feature collection using another feature collection as input"
)
class FastProximitySearch extends VectorProcess {

  private val log = Logger.getLogger(classOf[FastProximitySearch])

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
                 name = "dataFilter",
                 min = 0,
                 description = "The filter to apply to the dataFeatures collection")
               dataFilter: Filter,

               @DescribeParameter(
                 name = "bufferDistance",
                 description = "Buffer size in meters")
               bufferDistance: java.lang.Double

               ): SimpleFeatureCollection = {

    log.info("Fast Proximity Search on collection type " + dataFeatures.getClass.getName)

    if(!dataFeatures.isInstanceOf[AccumuloFeatureCollection]) {
      log.warn("The provided data feature collection type may not support fast proximity search: "+dataFeatures.getClass.getName)
    }

    val visitor = new FastProximityVisitor( inputFeatures,
                                            dataFeatures,
                                            Option(dataFilter).getOrElse(Filter.INCLUDE),
                                            bufferDistance)

    dataFeatures.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[ProximityResult].results
  }
}

class FastProximityVisitor( inputFeatures: SimpleFeatureCollection,
                            dataFeatures: SimpleFeatureCollection,
                            dataFilter: Filter,
                            bufferDistance: java.lang.Double
                          ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[FastProximityVisitor])

  val geoFac = new GeometryFactory
  val ff  = CommonFactoryFinder.getFilterFactory2

  def visit(feature: Feature): Unit = {
    // TODO if the feature collection is not an AccumuloFeatureCollection
    // revert to iterating through each feature for proximity search
  }

  var resultCalc: ProximityResult = new ProximityResult(new EmptyFeatureCollection(dataFeatures.getSchema))
  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = ProximityResult(r)

  def proximitySearch(source: SimpleFeatureSource, query: Query) = {

    log.info("Proximity searching on source type "+source.getClass.getName)

    import scala.collection.JavaConversions._
    val inputGeoms = inputFeatures.features().map { sf =>
      sf.getDefaultGeometry.asInstanceOf[Geometry].bufferMeters(bufferDistance)
    }
    val unionedGeom = geoFac.buildGeometry(inputGeoms.toSeq).union

    val geomProperty = ff.property(dataFeatures.getSchema.getGeometryDescriptor.getName)

    val geomFilters = (0 until unionedGeom.getNumGeometries).map { i =>
      ff.intersects(geomProperty, ff.literal(unionedGeom.getGeometryN(i)))
    }
    val geomOrFilter = ff.or(geomFilters)
    val combinedFilter = ff.and(List(query.getFilter, geomOrFilter, dataFilter))
    source.getFeatures(combinedFilter)
  }

}

case class ProximityResult(results: SimpleFeatureCollection) extends AbstractCalcResult