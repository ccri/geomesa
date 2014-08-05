package geomesa.utils.geohash

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control._

object NextJumpBS {

  def violations(min: GeoHash, max: GeoHash, cur: GeoHash): List[V] = {
    val list = ArrayBuffer[V]()

    var min_cand = -1
    var max_cand = -1

    var minfinished = false
    var maxfinished = false

    // x's
    var minbits = min.bitset
    var maxbits = max.bitset //GeoHash.gridIndexForLongitude(max)
    var curbits = cur.bitset //GeoHash.gridIndexForLongitude(cur)

    val xprec = min.prec / 2 + min.prec % 2

    println(s"xprec: $xprec\nminBits: $minbits ${minbits}" +
      s"\nmaxbits: $maxbits ${maxbits}" +
      s"\ncurbits: $curbits ${minbits}")

    for (p <- 0 until min.prec) {
      val curbit = curbits(p)
      val minbit = minbits(p)
      val maxbit = maxbits(p)

      println(s"*** P: $p curbit $curbit minbit: $minbit maxbit: $maxbit minfinished: $minfinished maxfinished: $maxfinished")

      if (!minfinished) {

        if (curbit == minbit && minbit && min_cand == -1) {
          list.append(V("x-min-1", p))
        } else if (curbit && !minbit && min_cand == -1) {
          min_cand = p
        } else if (minbit && min_cand >= 0) {
          list.append(V("x-min-2", min_cand))
          minfinished = true
          println("set minfinished")
        }
      }

      if (!maxfinished) {
        if (curbit == maxbit && !maxbit && max_cand == -1) {
          list.append(V("x-max-1", p))
        } else if (!curbit && maxbit && max_cand == -1) {
          max_cand = p
        } else if (!maxbit && max_cand >= 0) {
          list.append(V("x-max-2", max_cand))
          maxfinished = true
          println("set maxfinished")
        }
      }
    }
    list.toList
  }

  def njo(min: GeoHash, max: GeoHash, cur: GeoHash, violations: List[V]): GeoHash = {

    val njobs = new mutable.BitSet()

    var njo: GeoHash = null
    var tmp: GeoHash = null
    var c: Int = 0

    var vbetween: Boolean = false

    if(violations.isEmpty) return max

    var vs = violations
    val loop = new Breaks

    while (vs.nonEmpty) {
      val v = vs.head

      val dim = v.pos % 2

      vs = vs.tail

      if (!cur.bitset(v.pos)) {
        // Max-Violation
        println(s"Max violation at pos ${v.pos} in dim $dim: needs more work")

      } else {
        // Min-violation
        println(s"Min violation at pos ${v.pos} in dim $dim")

        c = if (vs.nonEmpty) {
          val q = vs.head; vs = vs.tail; q.pos
        } else v.pos

        loop.breakable {
          for (i <- v.pos + 1 to min.prec by 2) {
            println(s"In for loop: $i: cur: ${cur.bitset}")
            if (!cur.bitset(i)) {
              if (dim != i % 2 || vbetween) {

                println(s"I is $i: Starting loop ${cur.bitset} njobs: $njobs.")
                for (j <- 0 until min.prec) {
                  if (j >= i) {
                    if(cur.bitset(j)) njobs.add(j)
                  }
                  else njobs.add(j)

                }
                println(s"Built GH/returning $njobs.")
                return GeoHash(njobs, min.prec)

              } else if (c == i) {

                println("Breaking?")
                vs = new V("asd", i) +: vs
                loop.break
              }
            } else if (i == c && dim != i % 2) {
              println("Setting vbetween")
              vbetween = true
              if (vs.nonEmpty) {
                val q = vs.head
                vs = vs.tail
                c = q.pos

                println(s"Set c to $c")
              }
            }

          }
        }
        // if(i>MAXPOS) return max???

        println("Returning max for a great good")
        return max
      }
    }

    println("Should never get here?")
    njo
  }
}
