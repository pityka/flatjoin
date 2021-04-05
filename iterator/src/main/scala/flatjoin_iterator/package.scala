import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.StandardOpenOption
import java.nio.channels.FileChannel
import java.io.{Closeable, File}
import scala.collection.mutable.ArrayBuffer
import flatjoin._
import scala.language.postfixOps
package object flatjoin_iterator {

  val EmptyCloseable = new Closeable { def close = () }

  class PimpedFc(fc: FileChannel) {
    val writeBuffer =
      ByteBuffer.allocate(512 * 1024)
    val readBuffer =
      ByteBuffer.allocate(512 * 1024).order(ByteOrder.LITTLE_ENDIAN)
    readBuffer.flip
    def flushBuffer = {
      writeBuffer.flip
      while (writeBuffer.hasRemaining) {
        fc.write(writeBuffer)
      }
      writeBuffer.clear
    }
    def writeFully(bb: ByteBuffer) = {
      if (writeBuffer.remaining >= bb.remaining) {
        writeBuffer.put(bb)
      } else {
        flushBuffer
        writeBuffer.put(bb)
      }

    }
    def close = {
      flushBuffer
      fc.close
    }
    def writeInt(i: Int) = {
      val bb = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      bb.putInt(i)
      bb.flip
      writeFully(bb)
    }

    def rotate(bb: ByteBuffer) = {
      var i = 0
      while (bb.hasRemaining) {
        bb.put(i, bb.get)
        i += 1
      }
      bb.limit(bb.capacity)
      bb.position(i)
    }

    def fillRead = {
      rotate(readBuffer)
      var c = 0
      while (readBuffer.hasRemaining && c >= 0) {
        c = fc.read(readBuffer)
      }
      readBuffer.flip
    }
    def readFully(m: Int): Option[ByteBuffer] = {
      if (readBuffer.remaining < m) {
        fillRead
      }
      if (readBuffer.remaining < m) None
      else {
        Some(readBuffer)
      }
    }

    def readInt: Option[Int] = readFully(4).map(_.getInt)

    def writeFrame(bb: ByteBuffer) = {
      writeInt(bb.remaining)
      writeFully(bb)
    }

    private def readFrameLength: Option[Int] =
      readInt.flatMap { i =>
        readFully(i).map { _ =>
          i
        }
      }

    def elements[T: Format] = new Iterator[T] {
      var r: Option[Int] = None
      def fill = {
        r = readFrameLength
      }
      fill
      def hasNext = r.isDefined
      def next() = {
        val length = r.get
        val position = readBuffer.position
        val limit1 = readBuffer.limit
        readBuffer.limit(position + length)
        val t =
          try {
            implicitly[Format[T]].fromBytes(readBuffer)
          } finally {
            readBuffer.position(position + length)
            readBuffer.limit(limit1)
          }
        fill
        t
      }
    }

  }

  def openFileChannel(f: PimpedFc => Unit): File = {
    val file = File.createTempFile("sort", "sort")
    file.deleteOnExit
    val fc = new PimpedFc(
      FileChannel
        .open(file.toPath, StandardOpenOption.CREATE, StandardOpenOption.WRITE)
    )
    try {
      f(fc)
    } finally {
      fc.close
    }
    file
  }

  def merge[T: Ordering](i1: Iterator[T], i2: Iterator[T]): Iterator[T] =
    new Iterator[T] {

      var a1: Option[T] = None
      var a2: Option[T] = None

      def fill1 =
        if (i1.hasNext) {
          a1 = Some(i1.next)
        } else {
          a1 = None
        }
      def fill2 =
        if (i2.hasNext) {
          a2 = Some(i2.next)
        } else {
          a2 = None
        }

      fill1
      fill2

      def hasNext = {
        a1.isDefined || a2.isDefined
      }
      def next() =
        if (a1.isEmpty) {
          val r = a2.get
          fill2
          r
        } else if (a2.isEmpty) {
          val r = a1.get
          fill1
          r
        } else if (implicitly[Ordering[T]].lt(a1.get, a2.get)) {
          val r = a1.get
          fill1
          r
        } else {
          val r = a2.get
          fill2
          r
        }
    }

  def merge[T: Ordering](its: Seq[Iterator[T]]): Iterator[T] =
    its.reduce(merge(_, _))

  def write[T: Format](fc: PimpedFc, i: Iterator[T]) = {
    i.foreach { i =>
      val bb = implicitly[Format[T]].toBytes(i)
      fc.writeFrame(bb)
    }
    fc.flushBuffer
  }

  def writeTmp[T: Format](i: Iterator[T]): File =
    openFileChannel { fc =>
      i.foreach { i =>
        val bb = implicitly[Format[T]].toBytes(i)
        fc.writeFrame(bb)
      }
      fc.flushBuffer
    }

  def doMerge[T: Format: Ordering](l: List[File]): (Iterator[T], Closeable) = {
    val files = l.map { file =>
      val channel = FileChannel.open(file.toPath, StandardOpenOption.READ)
      val pc = new PimpedFc(channel)
      val iter = pc.elements[T]
      (file, channel, iter)
    }
    val closeable = new Closeable {
      def close = files.foreach { case (f, c, _) =>
        c.close
        f.delete
      }
    }
    merge(files.map(_._3)) -> closeable
  }

  def mergeFiles[T: Format: Ordering](
      l: List[File],
      max: Int
  ): (Iterator[T], Closeable) =
    if (l.size < max) doMerge(l)
    else
      mergeFiles(
        l.grouped(max)
          .map { group =>
            val (it, cl) = doMerge(group)
            val files = writeTmp(it)
            cl.close
            files
          }
          .toList,
        max
      )

  def sort[T: Format: Ordering](
      i: Iterator[T],
      max: Int
  ): (Iterator[T], Closeable) = {
    val sortedFiles = i
      .grouped(max)
      .map { group =>
        val array =
          new Array[AnyRef](
            group.size
          ) // Previously used ArraySeq for more compact but slower code
        var i = 0
        group.foreach { elem =>
          array(i) = elem.asInstanceOf[AnyRef]
          i += 1
        }

        java.util.Arrays
          .sort(array, implicitly[Ordering[T]].asInstanceOf[Ordering[Object]])

        writeTmp(array.asInstanceOf[Array[T]].iterator)
      }
      .toList
    mergeFiles(sortedFiles, 128)

  }

  def shard[T: StringKey: Format](it: Iterator[T]): List[File] = {
    val files = 0 until 128 map { i =>
      val file = File.createTempFile("shard_" + i, "shard")
      file.deleteOnExit
      val fc = FileChannel.open(
        file.toPath,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE
      )
      (i, (file, new PimpedFc(fc)))
    } toMap

    it.foreach { elem =>
      val key = implicitly[StringKey[T]].key(elem)
      val bb = implicitly[Format[T]].toBytes(elem)
      val hash = math.abs(scala.util.hashing.MurmurHash3.stringHash(key) % 128)

      val fc = files(hash)._2
      fc.writeFrame(bb)

    }
    files.foreach(_._2._2.close)
    files.map(_._2._1).toList
  }

  def adjacentSpan[T: Ordering](i: Iterator[T]): Iterator[Seq[T]] =
    new Iterator[Seq[T]] {

      val buffer = collection.mutable.ArrayBuffer[T]()

      val ordering = implicitly[Ordering[T]]

      var r: Option[T] = None

      def fill =
        if (i.hasNext) {
          r = Some(i.next)
        } else {
          r = None
        }

      def fillBuffer = {
        while (
          r.isDefined && (buffer.isEmpty || ordering.equiv(
            r.get,
            buffer.head
          ))
        ) {
          buffer.append(r.get)
          fill
        }
      }

      fill
      fillBuffer

      def hasNext = !buffer.isEmpty

      def next() = {
        val k = buffer.toList
        buffer.clear
        fillBuffer
        k
      }

    }

  def outerJoinInMemory[T: StringKey](
      it: Iterator[(Int, T)],
      columns: Int
  ): Iterable[Seq[Option[T]]] = {
    import scala.collection.mutable.ArrayBuffer
    val mmap =
      scala.collection.mutable.AnyRefMap[String, ArrayBuffer[(Int, T)]]()
    it.foreach { case pair =>
      val key = implicitly[StringKey[T]].key(pair._2)
      mmap.get(key) match {
        case None         => mmap.update(key, ArrayBuffer(pair))
        case Some(buffer) => buffer.append(pair)
      }
    }
    mmap.flatMap { case (_, group) =>
      crossGroup(group.toSeq, columns)
    }
  }

  def outerJoinSorted[T](sorted: Iterator[(Int, T)], columns: Int)(implicit
      ord: Ordering[(Int, T)]
  ): Iterator[Seq[Option[T]]] =
    adjacentSpan(sorted).flatMap { group =>
      crossGroup(group, columns)
    }

  def concatWithIndex[T](
      its: Iterator[(() => (Iterator[T], Closeable), Int)]
  ): Iterator[(Int, T)] =
    new Iterator[(Int, T)] {
      def fill =
        if (its.hasNext) {
          val (makeIter, idx) = its.next
          val (i, c) = makeIter.apply
          (i, c, idx)
        } else (Iterator.empty, EmptyCloseable, -1)

      var i: (Iterator[T], Closeable, Int) =
        fill

      def hasNext =
        if (i._3 < 0) false
        else {
          val b = i._1.hasNext
          if (!b) {
            i._2.close
            i = fill
            hasNext
          } else b
        }
      def next() = i._3 -> i._1.next
    }

  def sortAndOuterJoin[T: Ordering](
      its: List[() => (Iterator[T], Closeable)],
      max: Int
  )(implicit f: Format[(Int, T)]): (Iterator[Seq[Option[T]]], Closeable) = {
    val m: Iterator[(Int, T)] = concatWithIndex(its.zipWithIndex.iterator)

    implicit val ordering: Ordering[(Int, T)] = Ordering.by(_._2)
    if (m.hasNext) {
      val (sorted, close) = sort(m, max)
      val joined = outerJoinSorted(sorted, its.size)

      joined -> close
    } else Iterator.empty -> EmptyCloseable
  }

  def sort[T: Ordering](its: List[() => (Iterator[T], Closeable)], max: Int)(
      implicit f: Format[(Int, T)]
  ): (Iterator[(Int, T)], Closeable) = {
    val m: Iterator[(Int, T)] = concatWithIndex(its.zipWithIndex.iterator)

    implicit val ordering: Ordering[(Int, T)] = Ordering.by(_._2)
    if (m.hasNext) {
      sort(m, max)
    } else Iterator.empty -> EmptyCloseable
  }

  def outerJoin[T: StringKey](
      its: List[() => (Iterator[T], Closeable)],
      max: Int
  )(implicit f: Format[(Int, T)]): Iterator[Seq[Option[T]]] = {
    val m: Iterator[(Int, T)] = concatWithIndex(its.zipWithIndex.iterator)
    implicit val sk = new StringKey[(Int, T)] {
      def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
    }
    implicit val ordering: Ordering[(Int, T)] = Ordering.by(x => sk.key(x))
    if (m.hasNext) {
      val files = shard(m)
      val joinedIterators = files.iterator.map { file =>
        val open = () => {
          val channel = FileChannel.open(file.toPath, StandardOpenOption.READ)
          val iter = new PimpedFc(channel).elements[(Int, T)]
          val readClose = new Closeable {
            def close = channel.close; file.delete
          }
          val (sortedIter, sortClose) =
            if (iter.hasNext) sort(iter, max)
            else Iterator.empty -> EmptyCloseable
          val joined = outerJoinSorted(sortedIter, its.size)
          (
            joined,
            new Closeable {
              def close = { readClose.close; sortClose.close }
            }
          )

        }
        (open, 0)
      }
      concatWithIndex(joinedIterators).map(_._2)
    } else Iterator.empty
  }

  def outerJoinWithHashMap[T: StringKey](
      its: List[() => (Iterator[T], Closeable)]
  )(implicit f: Format[(Int, T)]): Iterator[Seq[Option[T]]] = {
    val m: Iterator[(Int, T)] = concatWithIndex(its.zipWithIndex.iterator)
    implicit val sk = new StringKey[(Int, T)] {
      def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
    }
    if (m.hasNext) {
      val files = shard(m)
      val joinedIterators = files.iterator.map { file =>
        val open = () => {
          val channel = FileChannel.open(file.toPath, StandardOpenOption.READ)
          val iter = new PimpedFc(channel).elements[(Int, T)]
          val joined = outerJoinInMemory(iter, its.size)
          channel.close
          file.delete
          (joined.iterator, EmptyCloseable)
        }
        (open, 0)
      }
      concatWithIndex(joinedIterators).map(_._2)
    } else Iterator.empty
  }

  def outerJoinFullyInMemory[T: StringKey](
      its: List[() => (Iterator[T], Closeable)]
  ): Iterable[Seq[Option[T]]] =
    outerJoinInMemory(concatWithIndex(its.zipWithIndex.iterator), its.size)

}
