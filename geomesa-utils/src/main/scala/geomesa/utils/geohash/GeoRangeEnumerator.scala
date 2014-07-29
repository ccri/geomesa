package geomesa.utils.geohash

import com.vividsolutions.jts.geom.{Point, Coordinate, PrecisionModel, GeometryFactory}
import scala.collection.BitSet
import scala.collection.immutable.{BitSet => IBitSet}
import com.typesafe.scalalogging.slf4j.Logging
import scala.collection.JavaConversions._

object GeoRangeEnumerator {
  def enumerateRanges(ll: Point, ur: Point, maxRes: Int): Seq[GeoHashRange] = {
    require(maxRes % 5 == 0)

    val llgh = GeoHash(ll, maxRes)
    val urgh = GeoHash(ur, maxRes)

    println(s"ll: ${llgh.hash}, ${llgh.bitset}")
    println(s"ur: ${urgh.hash}, ${urgh.bitset}")

    if(llgh == urgh) return Seq(GeoHashRange(llgh))

    val llh = llgh.hash
    val urh = urgh.hash
    //val sharedChars = llh.zip(urh).map{ case (a,b) => a == b }.indexOf(false) + 1

    val xor = llgh.bitset ^ urgh.bitset
    val firstDifferentBit = xor.head
    val sharedChars = firstDifferentBit / 5 + 1

    val llSharedRes = GeoHash(ll, sharedChars * 5)
    val urSharedRes = GeoHash(ur, sharedChars * 5)

    val bbi = new BoundingBoxGeoHashIterator(TwoGeoHashBoundingBox(llSharedRes, urSharedRes))

    GeoHashRange.getRanges(bbi.toList)
  }
}

case class GeoHashRange(start: GeoHash, end: GeoHash) {
  def canJoin(that: GeoHashRange): Boolean = end.next == that.start

  def join(that: GeoHashRange): Option[GeoHashRange] = if(canJoin(that)) {
    Some(GeoHashRange(start, that.end))
  } else {
    None
  }

  def join(gh: GeoHash): Seq[GeoHashRange] =
    if(end.next == gh) Seq(GeoHashRange(start, gh))
    else Seq(GeoHashRange(gh), this)

  override def toString =
    if(start != end) s"GeoHashRange(${start.hash}, ${start.bitset}-${end.hash}, ${end.bitset})"
    else             s"GeoHashRange(${start.hash}, ${start.bitset})"
}

object GeoHashRange {
  def apply(gh: GeoHash): GeoHashRange = GeoHashRange(gh, gh)

  def getRanges(seq: Seq[GeoHash]): Seq[GeoHashRange] = {
    seq match {
      case head :: tail => tail.foldLeft(Seq(GeoHashRange(head))) { case (s, gh) => s.head.join(gh) ++ s.tail}.reverse
      case Nil => Nil
    }
  }
}
