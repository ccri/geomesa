package org.locationtech.geomesa.memory.cqengine.utils

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.memory.cqengine.GeoCQEngine
import org.locationtech.geomesa.memory.cqengine.utils.SampleFeatures._
import org.opengis.filter.Filter
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class GeoCQEngineTest extends Specification {
  "GeoCQEngine" should {
    "return correct number of results" >> {
      import SampleFilters._
      val feats = (0 until 1000).map(SampleFeatures.buildFeature)

      // Set up CQEngine with geo-index only
      val cqNoIndexes = new GeoCQEngine(sft)
      cqNoIndexes.addAll(feats)

      // Set up CQEngine with all indexes
      val cqWithIndexes = new GeoCQEngine(sftWithIndexes)
      cqWithIndexes.addAll(feats)

      def getGeoToolsCount(filter: Filter) = feats.count(filter.evaluate)

      def getCQEngineCount(filter: Filter, cq: GeoCQEngine) = {
        cq.getReaderForFilter(filter).getIterator.size
      }

      def checkFilter(filter: Filter, cq: GeoCQEngine): MatchResult[Int] = {
        val gtCount = getGeoToolsCount(filter)

        val cqCount = getCQEngineCount(filter, cq)

        println(s"GT: $gtCount CQ: $cqCount Filter: $filter")

        // since GT count is (presumably) correct
        cqCount must equalTo(gtCount)
      }

      def runFilterTests(name: String, filters: Seq[Filter]) = {
        examplesBlock {
          for (f <- filters) {
            s"$name filter $f (geo-only index)" in {
              checkFilter(f, cqNoIndexes)
            }
            s"$name filter $f (various indices)" in {
              checkFilter(f, cqWithIndexes)
            }
          }
        }
      }

      runFilterTests("equality", equalityFilters)

      runFilterTests("special", specialFilters)

      runFilterTests("null", nullFilters)

      runFilterTests("comparable", comparableFilters)

      runFilterTests("temporal", temporalFilters)

      runFilterTests("one level AND", oneLevelAndFilters)

      runFilterTests("one level multiple AND", oneLevelMultipleAndsFilters)

      runFilterTests("one level OR", oneLevelOrFilters)

      runFilterTests("one level multiple OR", oneLevelMultipleOrsFilters)

      runFilterTests("one level NOT", simpleNotFilters)

      runFilterTests("basic spatial predicates", spatialPredicates)

      runFilterTests("attribute predicates", attributePredicates)


    }
  }
}
