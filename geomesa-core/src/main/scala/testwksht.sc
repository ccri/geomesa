/**
 * Created by anthony on 2/23/14.
 */

val i = 0

val j = i + 1

object test {

  case class Foo(i: Int)
}

import test._

val f = Foo(1)

f match {
  case f@(Foo(t)) if t > 0 => f
  case _ => Unit
}

