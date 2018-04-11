package flatjoin

import com.github.plokhotnyuk.jsoniter_scala.core._
import java.nio.ByteBuffer
import flatjoin_iterator._

object jsoniterformat {
  implicit def format[T: JsonValueCodec] = new Format[T] {
    def toBytes(t: T): ByteBuffer =
      ByteBuffer.wrap(writeToArray(t))
    def fromBytes(bb: ByteBuffer): T = {
      val ba = ByteBuffer.allocate(bb.remaining)
      while (ba.hasRemaining) {
        ba.put(bb.get)
      }
      readFromArray(ba.array)
    }
  }
}
