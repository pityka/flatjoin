package flatjoin

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import java.nio.ByteBuffer
import java.io.Closeable

import circeformat._
import flatjoin_iterator._

class CirceFlat extends FunSpec with ShouldMatchers {

  describe(" sort and join ") {
    it(" small test case ") {
      val a1 = List('a', 'a', 'b', 'c', 'd', 'e', 'h', 'h')
      val a2 = List('c', 'd', 'e', 'e', 'f', 'g', 'h')
      val a3 = List('e', 'f', 'g', 'h', 'h', 'i', 'j', 'k')

      val (i, c) = sortAndOuterJoin(List(a1, a2, Nil, a3).map(x =>
                                      () =>
                                        x.iterator -> new Closeable {
                                          def close = ()
                                    }),
                                    2)
      i.toList should equal(
        List(Vector(Some('a'), None, None, None),
             Vector(Some('a'), None, None, None),
             Vector(Some('b'), None, None, None),
             Vector(Some('c'), Some('c'), None, None),
             Vector(Some('d'), Some('d'), None, None),
             Vector(Some('e'), Some('e'), None, Some('e')),
             Vector(Some('e'), Some('e'), None, Some('e')),
             Vector(None, Some('f'), None, Some('f')),
             Vector(None, Some('g'), None, Some('g')),
             Vector(Some('h'), Some('h'), None, Some('h')),
             Vector(Some('h'), Some('h'), None, Some('h')),
             Vector(Some('h'), Some('h'), None, Some('h')),
             Vector(Some('h'), Some('h'), None, Some('h')),
             Vector(None, None, None, Some('i')),
             Vector(None, None, None, Some('j')),
             Vector(None, None, None, Some('k')))
      )
      c.close
    }

    it(" big test case ") {

      val N = 1000000
      val M = 100000
      val it1 = 0 to N iterator
      val it2 = 500 to (N + 500) iterator

      val (i, c) = sortAndOuterJoin(List(it1, it2).map(x =>
                                      () =>
                                        x -> new Closeable {
                                          def close = ()
                                    }),
                                    M)
      i.zipWithIndex.foreach {
        case (joined, idx) =>
          if (idx < 500) joined should equal(Vector(Some(idx), None))
          else if (idx > N) joined should equal(Vector(None, Some(idx)))
          else joined should equal(Vector(Some(idx), Some(idx)))
      }
      c.close
    }
  }

}
