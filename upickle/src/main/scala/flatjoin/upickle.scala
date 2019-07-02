package flatjoin

import upickle.default._
import java.nio.ByteBuffer

object upickleformat {
  implicit def format[T: Reader: Writer] = new Format[T] {
    def toBytes(t: T): ByteBuffer =
      ByteBuffer.wrap(upickle.default.write(t).getBytes("UTF-8"))
    def fromBytes(bb: ByteBuffer): T = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      val str = new String(ba.array)
      read[T](str)
    }
  }
}
