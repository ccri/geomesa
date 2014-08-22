package org.locationtech.geomesa.core.data

import org.opengis.feature.simple.SimpleFeature

package object tables {
  type TableWriter = SimpleFeature => Unit


}
