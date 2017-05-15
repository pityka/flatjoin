package flatjoin_fs2

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers
import java.nio.ByteBuffer
import java.io.Closeable

import scala.concurrent._
import scala.concurrent.duration._
import flatjoin._
import java.io.File

import fs2._
import scodec.bits.ByteVector

class Flat extends FunSpec with ShouldMatchers {

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

  val a1 = List("a", "a", "b", "c", "d", "e", "h", "h")
  val a2 = List("c", "d", "e", "e", "f", "g", "h")
  val a3 = List("e", "f", "g", "h", "h", "i", "j", "k")

  val expectedJoin = List(Vector(Some("a"), None, None, None),
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
                          Vector(None, None, None, Some("k")))

  val N = 1000000
  val M = 100000

  describe("file dump") {
    it("small") {
      val encode: Stream[Task, File] =
        Stream("abracadabra").covary[Task].through(dump)
      val decode: Pipe[Task, File, String] = readFrames[Task, String]
      encode.through(decode).runLog.unsafeRunSync should equal(
        Right(Vector("abracadabra")))

    }
  }
  describe("sort") {
    it("small") {
      val a1 = List("a", "a", "b", "c", "d", "e", "h", "h").reverse
      Stream(a1: _*)
        .covary[Task]
        .through(sort(2))
        .runLog
        .unsafeRunSync should equal(Right(a1.sorted.toVector))

    }
    it("small2") {

      val f =
        Stream(a1: _*).map { x =>
          println(x); x
        }.covary[Task].through(sort(5)).runLog.unsafeRunSync

      f should equal(Right(a1.toVector))
    }
  }

  describe("adjacentSpan") {
    it("small") {
      val a1 = List("a", "a", "b", "c", "d", "e", "h", "h")
      Stream(a1: _*)
        .covary[Task]
        .through(adjacentSpan(_ == _))
        .runLog
        .unsafeRunSync should equal(
        Right(
          Vector(Vector("a", "a"),
                 Vector("b"),
                 Vector("c"),
                 Vector("d"),
                 Vector("e"),
                 Vector("h", "h"))))
    }
  }

}
