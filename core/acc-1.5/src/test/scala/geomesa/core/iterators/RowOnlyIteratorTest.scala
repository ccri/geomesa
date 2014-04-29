package geomesa.core.iterators

import geomesa.core.Acc15VersionSpecificOperations
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

/**
 * Created by davidm on 4/29/14.
 */
// should run because of annotations on AbstractRowOnlyIteratorTest
class RowOnlyIteratorTest extends AbsractRowOnlyIteratorTest(Acc15VersionSpecificOperations)
