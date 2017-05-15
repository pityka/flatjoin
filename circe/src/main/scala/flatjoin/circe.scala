package flatjoin

import io.circe._
import io.circe.syntax._
import io.circe.parser._
import java.nio.ByteBuffer

object circeformat {
  implicit def format[T: Encoder: Decoder] = new Format[T] {
    def toBytes(t: T): ByteBuffer = {
      ByteBuffer.wrap(t.asJson.noSpaces.getBytes("UTF-8"))
    }
    def fromBytes(bb: ByteBuffer): T = {
      val ar = Array.ofDim[Byte](bb.remaining)
      bb.get(ar)
      val str = new String(ar, "UTF-8")
      parse(str).right.get.as[T].right.get
    }
  }
}
