package geomesa.core.iterators

import geomesa.core.Acc15VersionSpecificOperations
import geomesa.core.data.AccumuloDataStoreFactory
import org.specs2.runner.JUnitRunner
import org.junit.runner.RunWith

/**
 * Created by davidm on 4/29/14.
 */
@RunWith(classOf[JUnitRunner])
class DensityIteratorTest extends AbstractDensityIteratorTest(Acc15VersionSpecificOperations,
                                                           new AccumuloDataStoreFactory())
