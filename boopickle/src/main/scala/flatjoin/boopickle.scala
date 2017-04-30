package flatjoin

import boopickle.Default._
import java.nio.ByteBuffer

object boopickleformat {
  implicit def format[T: Pickler] = new Format[T] {
    def toBytes(t: T): ByteBuffer = Pickle.intoBytes(t)
    def fromBytes(bb: ByteBuffer): T = Unpickle[T].fromBytes(bb)
  }
}
