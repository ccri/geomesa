package org.locationtech.geomesa.jobs.index

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AttributeIndexJobTest extends Specification {


  "AccumuloIndexJob" should {




    "recreate a queryable attribute table for a stand-alone feature" in {

      // Add mediumFeatures as with unshared tables.

      // Query for attributes; check success.

      // Delete the Attribute table.

      // Run AttributeIndexJob

      // Query for attributes; check success.

    }

    "recreate a queryable attribute index for a shared-table feature" in {

      // Add mediumFeatures as with shared tables.

      // Query for attributes; check success.

      // Delete the Attribute table.

      // Run AttributeIndexJob

      // Query for attributes; check success.

    }

  }


}
