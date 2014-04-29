package geomesa.core.iterators

import geomesa.core.Acc14VersionSpecificOperations
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

/**
 * Created by davidm on 4/29/14.
 */
@RunWith(classOf[JUnitRunner])
class SurfaceAggregatingIteratorTest
    extends AbstractSurfaceAggregatingIteratorTest(Acc14VersionSpecificOperations)
