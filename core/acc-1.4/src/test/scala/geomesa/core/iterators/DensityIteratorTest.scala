package geomesa.core.iterators

import geomesa.core.Acc14VersionSpecificOperations
import geomesa.core.data.AccumuloDataStoreFactory
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

/**
 * Created by davidm on 4/29/14.
 */
@RunWith(classOf[JUnitRunner])
class DensityIteratorTest extends AbstractDensityIteratorTest(Acc14VersionSpecificOperations,
                                                              new AccumuloDataStoreFactory())
