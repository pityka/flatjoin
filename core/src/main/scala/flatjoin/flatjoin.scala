package flatjoin

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.StandardOpenOption
import java.nio.channels.FileChannel
import java.io.{Closeable, File}

trait Format[T] {
  def toBytes(t: T): ByteBuffer
  def fromBytes(bb: ByteBuffer): T
}

trait StringKey[T] {
  def key(t: T): String
}
