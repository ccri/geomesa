package org.locationtech.geomesa.core.index

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.{DATA_CQ, SimpleFeatureEncoder}
import org.locationtech.geomesa.utils.geohash.{GeoHash, GeohashUtils}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object IndexEntry {

  val timeZone = DateTimeZone.forID("UTC")

  implicit class IndexEntrySFT(sf: SimpleFeature) {
    lazy val userData = sf.getFeatureType.getUserData
    lazy val dtgStartField = userData.getOrElse(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME).asInstanceOf[String]
    lazy val dtgEndField = userData.getOrElse(SF_PROPERTY_END_TIME, DEFAULT_DTG_END_PROPERTY_NAME).asInstanceOf[String]

    lazy val sid = sf.getID
    lazy val gh: GeoHash = GeohashUtils.reconstructGeohashFromGeometry(geometry)
    def geometry = sf.getDefaultGeometry match {
      case geo: Geometry => geo
      case other =>
        throw new Exception(s"Default geometry must be Geometry: '$other' of type '${Option(other).map(_.getClass).orNull}'")
    }

    private def getTime(attr: String) = sf.getAttribute(attr).asInstanceOf[java.util.Date]
    def startTime = getTime(dtgStartField)
    def endTime   = getTime(dtgEndField)
    lazy val dt   = Option(startTime).map { d => new DateTime(d) }

    private def setTime(attr: String, time: DateTime) =
      sf.setAttribute(attr, Option(time).map(_.toDate).orNull)

    def setStartTime(time: DateTime) = setTime(dtgStartField, time)
    def setEndTime(time: DateTime)   = setTime(dtgEndField, time)
  }

}

case class IndexEncoder(rowf: TextFormatter,
                        cff: TextFormatter,
                        cqf: TextFormatter,
                        featureEncoder: SimpleFeatureEncoder) 
  extends Logging {

  import org.locationtech.geomesa.core.index.IndexEntry._
  import org.locationtech.geomesa.utils.geohash.GeohashUtils._

  val formats = Array(rowf,cff,cqf)

  // the resolutions are valid for decomposed objects are all 5-bit boundaries
  // between 5-bits and 35-bits (inclusive)
  lazy val decomposableResolutions: ResolutionRange = new ResolutionRange(0, 35, 5)

  // the maximum number of sub-units into which a geometry may be decomposed
  lazy val maximumDecompositions: Int = 5

  val getRCFCQ = (geohash: GeoHash, dt: DateTime, featureToEncode: SimpleFeature) =>
    Array(
      rowf.format(geohash, dt, featureToEncode),
      cff.format(geohash, dt, featureToEncode),
      cqf.format(geohash, dt, featureToEncode)
    )

  def encode(featureToEncode: SimpleFeature, visibility: String = ""): List[KeyValuePair] = {

    logger.trace(s"encoding feature: $featureToEncode")

    // decompose non-point geometries into multiple index entries
    // (a point will return a single GeoHash at the maximum allowable resolution)
    val geohashes =
      decomposeGeometry(featureToEncode.geometry, maximumDecompositions, decomposableResolutions)

    logger.trace(s"decomposed ${featureToEncode.geometry} into geohashes: ${geohashes.map(_.hash).mkString(",")})}")

    val v = new Text(visibility)
    val dt = featureToEncode.dt.getOrElse(new DateTime()).withZone(timeZone)

    val id = new Text(featureToEncode.sid)
    val indexValue = IndexSchema.encodeIndexValue(featureToEncode)
    val iv = new Value(indexValue)

    // the (single) data value is the encoded (serialized-to-string) SimpleFeature
    val dataValue = new Value(featureEncoder.encode(featureToEncode))

    // remember the resulting index-entries
//    val keys = geohashes.map { gh =>
//      val Array(r, cf, cq) = formats.map { _.format(gh, dt, featureToEncode) }
//      new Key(r, cf, cq, v)
//    }
//    val rowIDs = keys.map(_.getRow)

    // the index entries are (key, FID) pairs
//    val indexEntries: List[(Key, Value)] = keys.map { k => (k, iv) }

    // data entries are stored separately (and independently) from the index entries;
    // each attribute gets its own data row (though currently, we use only one attribute
    // that represents the entire, encoded feature)
//    val dataEntries: List[(Key, Value)] = rowIDs.map { rowID =>
//      val key = new Key(rowID, id, DATA_CQ, v)
//      (key, dataValue)
//    }

    val ret = new Array[(Key, Value)](geohashes.size * 2)

    for (i <- 0 until geohashes.size) {
      val indexEntryIndex = i * 2
      val dataEntryIndex = indexEntryIndex + 1

      val gh = geohashes(i)

      val Array(r, cf, cq) = getRCFCQ(gh, dt, featureToEncode)
      val indexKey = new Key(r, cf, cq, v)

      ret(indexEntryIndex) = (indexKey, iv)

      val dataKey = new Key(r, id, DATA_CQ, v)
      ret(dataEntryIndex) = (dataKey, dataValue)
    }

    ret.toList
  }

}

case class IndexEntryDecoder(ghDecoder: GeohashDecoder,
                             dtDecoder: Option[DateDecoder]) {

  def decode(key: Key) =
    SimpleFeatureBuilder.build(indexSFT, List(ghDecoder.decode(key).geom, dtDecoder.map(_.decode(key))), "")
}