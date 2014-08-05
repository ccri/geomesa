package geomesa.utils.geohash

import scala.collection.mutable.ArrayBuffer

case class V(dim: String, pos: Int)

object NextJump {

  def njo(min: GeoHash, max: GeoHash, cur: GeoHash): List[V] = {
    val list = ArrayBuffer[V]()

    var min_cand = -1
    var max_cand = -1

    var minfinished = false
    var maxfinished = false

    // x's
    var minbits = GeoHash.gridIndexForLongitude(min)
    var maxbits = GeoHash.gridIndexForLongitude(max)
    var curbits = GeoHash.gridIndexForLongitude(cur)

    val xprec = min.prec / 2 + min.prec % 2

    println(s"xprec: $xprec\nminBits: $minbits ${minbits.toBinaryString}" +
      s"\nmaxbits: $maxbits ${maxbits.toBinaryString}" +
      s"\ncurbits: $curbits ${minbits.toBinaryString}")

    for(p <-0 until xprec) {
      val curbit = curbits & (1 << (xprec-p-1))
      val minbit = minbits & (1 << (xprec-p-1))
      val maxbit = maxbits & (1 << (xprec-p-1))

      println(s"\n*** P: $p curbit $curbit minbit: $minbit maxbit: $maxbit minfinished: $minfinished maxfinished: $maxfinished")

      if(!minfinished) {

        if (curbit == minbit && minbit > 0 && min_cand == -1) {
          list.append(V("x-min-1", p))
        } else if (curbit > 0 && minbit == 0 && min_cand == -1) {
          min_cand = p
        } else if (minbit > 0 && min_cand >= 0) {
          list.append(V("x-min-2", min_cand))
          minfinished = true
          println("set minfinished")
        }
      }

      if(!maxfinished) {
        if(curbit == maxbit && maxbit == 0 && max_cand == -1) {
          list.append(V("x-max-1", p))
        } else if(curbit == 0 && maxbit > 0 && max_cand == -1) {
          max_cand = p
        } else if(maxbit == 0 && max_cand >= 0) {
          list.append(V("x-max-2", max_cand))
          maxfinished = true
          println("set maxfinished")
        }
      }
    }
    list.toList
  }
}


//    minbits = GeoHash.gridIndexForLatitude(min)
//    maxbits = GeoHash.gridIndexForLatitude(max)
//    curbits = GeoHash.gridIndexForLatitude(cur)
//
//    min_cand = -1
//    max_cand = -1
//
//    minfinished = false
//    maxfinished = false
//
//    val yprec = min.prec/2
//
//    for(p <- 0 until yprec) {
//      val curbit = curbits ^ (1 << (yprec-p))
//      val minbit = minbits ^ (1 << (yprec-p))
//      val maxbit = maxbits ^ (1 << (yprec-p))
//
//      if(!minfinished) {
//
//        if(curbit == minbit && minbit >= 0 && min_cand == -1) {
//          list.append(p)
//        } else if(curbit > 0 && minbit == 0 && min_cand == -1) {
//          min_cand = p
//        } else if(minbit > 0 && min_cand >= 0) {
//          list.append(min_cand)
//          minfinished = true
//        }
//
//        if(!maxfinished) {
//          if(curbit == maxbit && maxbit == 0 && max_cand == -1) {
//            list.append(p)
//          } else if(curbit == 0 && maxbit > 0 && max_cand == -1) {
//            max_cand = p
//          } else if(maxbit > 0 && max_cand >= 0) {
//            list.append(p)
//            maxfinished = true
//          }
//        }
//
//      }
//    }

//    list.toList.sorted
//  }
//}
