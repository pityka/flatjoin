package flatjoin_akka

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scala.language.postfixOps
import java.nio.ByteBuffer
import java.io.Closeable

import akka._
import akka.util.ByteString
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import scala.concurrent._
import scala.concurrent.duration._
import flatjoin._
import java.io.File

class Flat extends AnyFunSpec with Matchers {

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

  implicit val formatString2 = new Format[String] {
    def toBytes(e: String): ByteBuffer = {
      val str = e.getBytes("UTF-8")
      ByteBuffer.wrap(str)
    }
    def fromBytes(bb: ByteBuffer): String = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      new String(ba.array)

    }
  }

  implicit val formatInt2 = new Format[(Int, Int)] {
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
  implicit val formatInt = new Format[Int] {
    def toBytes(e: Int): ByteBuffer = {
      val str = e.toString.getBytes("UTF-8")
      ByteBuffer.wrap(str)
    }
    def fromBytes(bb: ByteBuffer): Int = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      val str = new String(ba.array)
      str.toInt
    }
  }

  implicit val skInt = new StringKey[Int] {
    def key(t: Int) = t.toString
  }

  implicit val as = ActorSystem()
  implicit val am = ActorMaterializer()
  import am.executionContext

  val a1 = List("a", "a", "b", "c", "d", "e", "h", "h")
  val a2 = List("c", "d", "e", "e", "f", "g", "h")
  val a3 = List("e", "f", "g", "h", "h", "i", "j", "k")

  val expectedJoin = List(
    Seq(
      Vector(Some("a"), None, None, None),
      Vector(Some("a"), None, None, None)
    ),
    Seq(Vector(Some("b"), None, None, None)),
    Seq(Vector(Some("c"), Some("c"), None, None)),
    Seq(Vector(Some("d"), Some("d"), None, None)),
    Seq(
      Vector(Some("e"), Some("e"), None, Some("e")),
      Vector(Some("e"), Some("e"), None, Some("e"))
    ),
    Seq(Vector(None, Some("f"), None, Some("f"))),
    Seq(Vector(None, Some("g"), None, Some("g"))),
    Seq(
      Vector(Some("h"), Some("h"), None, Some("h")),
      Vector(Some("h"), Some("h"), None, Some("h")),
      Vector(Some("h"), Some("h"), None, Some("h")),
      Vector(Some("h"), Some("h"), None, Some("h"))
    ),
    Seq(Vector(None, None, None, Some("i"))),
    Seq(Vector(None, None, None, Some("j"))),
    Seq(Vector(None, None, None, Some("k")))
  )
  val expectedInnerJoin = List(
    Seq(
      Vector(Some("e"), Some("e"), None, Some("e")),
      Vector(Some("e"), Some("e"), None, Some("e"))
    ),
    Seq(
      Vector(Some("h"), Some("h"), None, Some("h")),
      Vector(Some("h"), Some("h"), None, Some("h")),
      Vector(Some("h"), Some("h"), None, Some("h")),
      Vector(Some("h"), Some("h"), None, Some("h"))
    )
  )

  val N = 1000000
  val M = 100000

  def it1 = Source(0 to N)
  def it2 = Source(500 to (N + 500))

  describe("sort") {
    it("small") {
      val a1 = List("a", "a", "b", "c", "d", "e", "h", "h").reverse
      val sortFlow = Instance().sort[String]
      val f =
        Await.result(Source(a1).via(sortFlow).runWith(Sink.seq), 20 seconds)
      f should equal(List("a", "a", "b", "c", "d", "e", "h", "h"))
    }
  }

  describe("bucket sort") {
    it("small") {
      val a1 = List("a", "a", "b", "c", "d", "e", "h", "h").reverse
      val sortFlow = Instance().bucketSort[String](_ match {
        case "a" =>
          "b"
        case _ =>
          "a"
      })
      val f =
        Await.result(Source(a1).via(sortFlow).runWith(Sink.seq), 10 seconds)
      f should equal(List("b", "c", "d", "e", "h", "h", "a", "a"))
    }
  }

  describe("span") {
    it("small") {

      val a1 = List("a", "a", "b", "c", "d", "e", "h", "h")
      val f =
        Await.result(
          Source(a1).via(adjacentSpan[String]).runWith(Sink.seq),
          20 seconds
        )
      f should equal(
        List(
          List("a", "a"),
          List("b"),
          List("c"),
          List("d"),
          List("e"),
          List("h", "h")
        )
      )

    }
  }

  describe("sort and outer join ") {
    it("small") {
      val sources = List(a1, a2, Nil, a3).map(x => Source(x))

      val f =
        Await
          .result(
            concatSources(sources)
              .via(Instance().sortAndOuterJoin(4))
              .runWith(Sink.seq),
            20 seconds
          )
          .map(_.toVector)
          .toList
      f should equal(expectedJoin)

    }
    it("big") {

      Await.result(
        concatSources(List(it1, it2))
          .via(Instance().sortAndOuterJoin(2))
          .mapConcat(_.toList)
          .runForeach { joined =>
            val idx = joined.find(_.isDefined).get.get
            if (idx < 500) joined(1) should equal(None)
            else if (idx > N) joined(0) should equal(None)
            else joined(0).get should equal(joined(1).get)
          },
        60 seconds
      )

    }
  }

  describe("shard and outer join ") {
    it("small") {
      val sources = List(a1, a2, Nil, a3).map(x => Source(x))

      val f =
        Await
          .result(
            concatSources(sources)
              .via(Instance().outerJoinBySortingShards(4))
              .runWith(Sink.seq),
            20 seconds
          )
          .map(_.toVector)
          .toList
      f.sortBy(_.hashCode) should equal(expectedJoin.sortBy(_.hashCode))

    }
    it("big") {

      Await.result(
        concatSources(List(it1, it2))
          .via(Instance().outerJoinBySortingShards(2))
          .mapConcat(_.toList)
          .runForeach { joined =>
            val idx = joined.find(_.isDefined).get.get
            if (idx < 500) joined(1) should equal(None)
            else if (idx > N) joined(0) should equal(None)
            else joined(0).get should equal(joined(1).get)
          },
        60 seconds
      )

    }
  }

  describe("shard and outer join in memory") {
    it("small") {
      val sources = List(a1, a2, Nil, a3).map(x => Source(x))

      val f =
        Await
          .result(
            concatSources(sources)
              .via(Instance().joinByShards(4))
              .runWith(Sink.seq),
            10 seconds
          )
          .map(_.toVector)
          .toList
      f.sortBy(_.hashCode) should equal(expectedJoin.sortBy(_.hashCode))

    }
    it("big") {

      Await.result(
        concatSources(List(it1, it2))
          .via(Instance().joinByShards[Int](2))
          .mapConcat(_.toList)
          .runForeach { joined =>
            val idx = joined.find(_.isDefined).get.get
            if (idx < 500) joined(1) should equal(None)
            else if (idx > N) joined(0) should equal(None)
            else joined(0).get should equal(joined(1).get)
          },
        60 seconds
      )

    }
  }
  describe("shard and inner join in memory") {
    it("small") {
      val sources = List(a1, a2, Nil, a3).map(x => Source(x))

      val f =
        Await
          .result(
            concatSources(sources)
              .via(Instance().joinByShards(4, List(0, 1, 3)))
              .runWith(Sink.seq),
            10 seconds
          )
          .map(_.toVector)
          .toList
      f.sortBy(_.hashCode) should equal(expectedInnerJoin.sortBy(_.hashCode))

    }
    it("big 1") {

      Await.result(
        concatSources(List(it1, it2))
          .via(Instance().joinByShards[Int](2, List(1)))
          .mapConcat(_.toList)
          .runForeach { joined =>
            val idx = joined.find(_.isDefined).get.get
            if (idx > N) joined(0) should equal(None)
            else joined(0).get should equal(joined(1).get)
          },
        60 seconds
      )

    }
    it("big 0") {

      Await.result(
        concatSources(List(it1, it2))
          .via(Instance().joinByShards[Int](2, List(0)))
          .mapConcat(_.toList)
          .runForeach { joined =>
            val idx = joined.find(_.isDefined).get.get
            if (idx < 500) joined(1) should equal(None)
            else joined(0).get should equal(joined(1).get)
          },
        60 seconds
      )

    }
  }

  describe("group") {
    it("small") {
      val source = Source(List("a", "a", "b", "b", "b", "a"))

      Await
        .result(
          source.via(Instance().groupBySortingShards).runWith(Sink.seq),
          atMost = 5 seconds
        )
        .toSet should equal(Set(List("a", "a", "a"), List("b", "b", "b")))
    }
  }

}
