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

package org.locationtech.geomesa.utils.ingest

import org.apache.commons.compress.compressors.bzip2.BZip2Utils
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.XZUtils

object Formats {
  val CSV     = "csv"
  val TSV     = "tsv"
  val TIFF    = "tiff"
  val DTED    = "dted"
  val SHP     = "shp"
  val JSON    = "json"
  val GeoJson = "geojson"
  val GML = "gml"

  def getFileExtension(name: String) = {
    val fileExtension = name match {
      case _ if GzipUtils.isCompressedFilename(name)  => GzipUtils.getUncompressedFilename(name)
      case _ if BZip2Utils.isCompressedFilename(name) => BZip2Utils.getUncompressedFilename(name)
      case _ if XZUtils.isCompressedFilename(name)    => XZUtils.getUncompressedFilename(name)
      case _ => name
    }

    fileExtension.toLowerCase match {
      case _ if fileExtension.endsWith(CSV)  => CSV
      case _ if fileExtension.endsWith("tif") ||
                fileExtension.endsWith("tiff") => TIFF
      case _ if fileExtension.endsWith("dt0") ||
                fileExtension.endsWith("dt1") ||
                fileExtension.endsWith("dt2")=> DTED
      case _ if fileExtension.endsWith(TSV)  => TSV
      case _ if fileExtension.endsWith(SHP)  => SHP
      case _ if fileExtension.endsWith(JSON) => JSON
      case _ if fileExtension.endsWith(GML)  => GML
      case _ => "unknown"
    }
  }

  val All = List(CSV, TSV, SHP, JSON, GeoJson, GML)
}
