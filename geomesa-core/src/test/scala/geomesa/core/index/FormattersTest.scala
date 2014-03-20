/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package geomesa.core.index

import com.vividsolutions.jts.geom._
import geomesa.utils.text.WKTUtils
import org.apache.accumulo.core.data.Key
import org.geotools.data.DataUtilities
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.util.Try
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.opengis.feature.simple.SimpleFeature
import java.util.Date

@RunWith(classOf[JUnitRunner])
class FormattersTest extends Specification {
  "PartitionTextFormatter" should {
    val numTrials = 100

    val featureType = DataUtilities.createType("TestFeature",
      "the_geom:Point,lat:Double,lon:Double,date:String")
    val featureWithId = DataUtilities.createFeature(
      featureType, "fid1=POINT(-78.1 38.2)|38.2|-78.1|2014-03-20T07:28:00.0Z" )

    val partitionTextFormatter = PartitionTextFormatter[SimpleFeature](99)

    "map features with non-null identifiers to fixed partitions" in {
      val shardNumbers = (1 to numTrials).map(trial =>
        partitionTextFormatter.format(featureWithId)
      ).toSet

      shardNumbers.size must be equalTo 1
    }

    // NB:  As desirable as it might be to have a test for features
    // with null IDs, there was no obvious way to create one.
  }
}
