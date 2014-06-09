package geomesa.core.process.query

import com.vividsolutions.jts.geom.Geometry
import geomesa.core.data.AccumuloFeatureCollection
import geomesa.utils.geotools.Conversions._
import org.apache.log4j.Logger
import org.geotools.data.Query
import org.geotools.data.simple.{SimpleFeatureSource, SimpleFeatureCollection}
import org.geotools.data.store.EmptyFeatureCollection
import org.geotools.feature.visitor.{FeatureCalc, CalcResult, AbstractCalcResult}
import org.geotools.process.factory.{DescribeParameter, DescribeResult, DescribeProcess}
import org.geotools.process.vector.VectorProcess
import org.geotools.util.NullProgressListener
import org.opengis.feature.Feature
import org.opengis.filter.Filter
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.feature.DefaultFeatureCollection
import org.opengis.feature.simple.SimpleFeature
import org.geotools.factory.CommonFactoryFinder

@DescribeProcess(
  title = "Fast Proximity Search on Feature Collections",
  description = "Performs a proximity search on a feature collection using another feature collection as input"
)
class GeomesaQuery extends VectorProcess {

  private val log = Logger.getLogger(classOf[GeomesaQuery])

  @DescribeResult(description = "Output feature collection")
  def execute(
               @DescribeParameter(
                 name = "features",
                 description = "The feature set on which to query")
               features: SimpleFeatureCollection,

               @DescribeParameter(
                 name = "filter",
                 min = 0,
                 description = "The filter to apply to the features collection")
               filter: Filter
               ): SimpleFeatureCollection = {

    log.info("Attempting Geomesa query on type " + features.getClass.getName)

    val visitor = new GeomesaQueryVisitor(features, Option(filter).getOrElse(Filter.INCLUDE))
    features.accepts(visitor, new NullProgressListener)
    visitor.getResult.asInstanceOf[GeomesaQueryResult].results
  }
}

class GeomesaQueryVisitor( features: SimpleFeatureCollection,
                           filter: Filter
                           ) extends FeatureCalc {

  private val log = Logger.getLogger(classOf[GeomesaQueryVisitor])

  val manualVisitResults = new DefaultFeatureCollection(null, features.getSchema)
  val ff  = CommonFactoryFinder.getFilterFactory2

  // Called for non AccumuloFeactureCollections
  def visit(feature: Feature): Unit = {
    val sf = feature.asInstanceOf[SimpleFeature]
    if(filter.evaluate(sf)) {
      manualVisitResults.add(sf)
    }
  }

  var resultCalc: GeomesaQueryResult = new GeomesaQueryResult(manualVisitResults)

  override def getResult: CalcResult = resultCalc

  def setValue(r: SimpleFeatureCollection) = resultCalc = GeomesaQueryResult(r)

  def query(source: SimpleFeatureSource, query: Query) = {
    log.info("Running Geomesa query on source type "+source.getClass.getName)
    val combinedFilter = ff.and(query.getFilter, filter)
    source.getFeatures(combinedFilter)
  }

}

case class GeomesaQueryResult(results: SimpleFeatureCollection) extends AbstractCalcResult