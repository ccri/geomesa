/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.utils.formats

object Formats {
  val CSV     = "csv"
  val TSV     = "tsv"
  val TIFF    = "tiff"
  val DTED    = "dted"
  val SHP     = "shp"
  val JSON    = "json"
  val GeoJson = "geojson"
  val GML = "gml"

  def getFileExtension(name: String) =
    name.toLowerCase match {
      case _ if name.endsWith(CSV)  => CSV
      case _ if name.endsWith("tif") ||
                name.endsWith("tiff") => TIFF
      case _ if name.endsWith("dt0") ||
                name.endsWith("dt1") ||
                name.endsWith("dt2")=> DTED
      case _ if name.endsWith(TSV)  => TSV
      case _ if name.endsWith(SHP)  => SHP
      case _ if name.endsWith(JSON) => JSON
      case _ if name.endsWith(GML)  => GML
      case _                        => "unknown"
    }

  val All = List(CSV, TSV, SHP, JSON, GeoJson, GML)
}
