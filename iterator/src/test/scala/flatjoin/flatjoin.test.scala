package flatjoin

import org.scalatest.FunSpec
import org.scalatest.Matchers
import java.nio.ByteBuffer
import java.io.Closeable

class Flat extends FunSpec with Matchers {

  implicit val skString = new StringKey[String] {
    def key(t: String) = t.toString
  }
  implicit val formatString = new Format[(Int, String)] {
    def toBytes(e: (Int, String)): ByteBuffer = {
      val str = (e._1.toString + "," + e._2).getBytes("UTF-8")
      ByteBuffer.wrap(str)
    }
    def fromBytes(bb: ByteBuffer): (Int, String) = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      val str = new String(ba.array)
      val spl = str.split(",")
      spl(0).toInt -> spl(1)
    }
  }

  implicit val formatInt = new Format[(Int, Int)] {
    def toBytes(e: (Int, Int)): ByteBuffer = {
      val str = (e._1.toString + "," + e._2).getBytes("UTF-8")
      ByteBuffer.wrap(str)
    }
    def fromBytes(bb: ByteBuffer): (Int, Int) = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      val str = new String(ba.array)
      val spl = str.split(",")
      spl(0).toInt -> spl(1).toInt
    }
  }

  implicit val skChar = new StringKey[Char] {
    def key(t: Char) = t.toString
  }
  implicit val formatChar = new Format[(Int, Char)] {
    def toBytes(e: (Int, Char)): ByteBuffer = {
      val str = (e._1.toString + "," + e._2).getBytes("UTF-8")
      ByteBuffer.wrap(str)
    }
    def fromBytes(bb: ByteBuffer): (Int, Char) = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      val str = new String(ba.array)
      val spl = str.split(",")
      spl(0).toInt -> spl(1).head
    }
  }

  implicit val skInt = new StringKey[Int] {
    def key(t: Int) = t.toString
  }

  describe(" hashmap ") {
    it(" small test case ") {
      val a1 = List("a", "a", "b", "c", "d", "e", "h", "h")
      val a2 = List("c", "d", "e", "e", "f", "g", "h")
      val a3 = List("e", "f", "g", "h", "h", "i", "j", "k")

      outerJoinFullyInMemory(List(a1, a2, Nil, a3).map(x =>
        () =>
          x.iterator -> new Closeable {
            def close = ()
      })).toList.sortBy(_.hashCode) should equal(
        List(
          Vector(Some("a"), None, None, None),
          Vector(Some("a"), None, None, None),
          Vector(Some("b"), None, None, None),
          Vector(Some("c"), Some("c"), None, None),
          Vector(Some("d"), Some("d"), None, None),
          Vector(Some("e"), Some("e"), None, Some("e")),
          Vector(Some("e"), Some("e"), None, Some("e")),
          Vector(None, Some("f"), None, Some("f")),
          Vector(None, Some("g"), None, Some("g")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(None, None, None, Some("i")),
          Vector(None, None, None, Some("j")),
          Vector(None, None, None, Some("k"))
        ).sortBy(_.hashCode)
      )
    }
    it(" big test case ") {

      val N = 1000000
      val M = 100000
      val it1 = 0 to N iterator
      val it2 = 500 to (N + 500) iterator

      outerJoinFullyInMemory(List(it1, it2).map(x =>
        () =>
          x -> new Closeable {
            def close = ()
      })).foreach {
        case (joined) =>
          val idx = joined.find(_.isDefined).get.get
          if (idx < 500) joined(1) should equal(None)
          else if (idx > N) joined(0) should equal(None)
          else joined(0).get should equal(joined(1).get)
      }
    }
  }

  describe(" shard and hashmap ") {
    it(" small test case ") {
      val a1 = List("a", "a", "b", "c", "d", "e", "h", "h")
      val a2 = List("c", "d", "e", "e", "f", "g", "h")
      val a3 = List("e", "f", "g", "h", "h", "i", "j", "k")

      outerJoinWithHashMap(List(a1, a2, Nil, a3).map(x =>
        () =>
          x.iterator -> new Closeable {
            def close = ()
      })).toList.sortBy(_.hashCode) should equal(
        List(
          Vector(Some("a"), None, None, None),
          Vector(Some("a"), None, None, None),
          Vector(Some("b"), None, None, None),
          Vector(Some("c"), Some("c"), None, None),
          Vector(Some("d"), Some("d"), None, None),
          Vector(Some("e"), Some("e"), None, Some("e")),
          Vector(Some("e"), Some("e"), None, Some("e")),
          Vector(None, Some("f"), None, Some("f")),
          Vector(None, Some("g"), None, Some("g")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(Some("h"), Some("h"), None, Some("h")),
          Vector(None, None, None, Some("i")),
          Vector(None, None, None, Some("j")),
          Vector(None, None, None, Some("k"))
        ).sortBy(_.hashCode)
      )
    }
    it(" big test case ") {

      val N = 1000000
      val M = 100000
      val it1 = 0 to N iterator
      val it2 = 500 to (N + 500) iterator

      outerJoinWithHashMap(List(it1, it2).map(x =>
        () =>
          x -> new Closeable {
            def close = ()
      })).foreach {
        case (joined) =>
          val idx = joined.find(_.isDefined).get.get
          if (idx < 500) joined(1) should equal(None)
          else if (idx > N) joined(0) should equal(None)
          else joined(0).get should equal(joined(1).get)
      }
    }
  }

  describe(" shard and sort ") {
    it(" small test case ") {
      val a1 = List('a', 'a', 'b', 'c', 'd', 'e', 'h', 'h')
      val a2 = List('c', 'd', 'e', 'e', 'f', 'g', 'h')
      val a3 = List('e', 'f', 'g', 'h', 'h', 'i', 'j', 'k')

      outerJoin(List(a1, a2, Nil, a3).map(x =>
                  () =>
                    x.iterator -> new Closeable {
                      def close = ()
                }),
                2).toList.sortBy(_.hashCode) should equal(
        List(
          Vector(Some('a'), None, None, None),
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
          Vector(None, None, None, Some('k'))
        ).sortBy(_.hashCode)
      )
    }
    it(" big test case ") {

      val N = 1000000
      val M = 100000
      val it1 = 0 to N iterator
      val it2 = 500 to (N + 500) iterator

      outerJoin(List(it1, it2).map(x =>
                  () =>
                    x -> new Closeable {
                      def close = ()
                }),
                M).foreach {
        case (joined) =>
          val idx = joined.find(_.isDefined).get.get
          if (idx < 500) joined(1) should equal(None)
          else if (idx > N) joined(0) should equal(None)
          else joined(0).get should equal(joined(1).get)
      }
    }
  }

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
        List(
          Vector(Some('a'), None, None, None),
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
          Vector(None, None, None, Some('k'))
        )
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
