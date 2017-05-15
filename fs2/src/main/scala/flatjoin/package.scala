import scala.concurrent._
import flatjoin._
import java.io.File

import fs2._
// import fs2.Task
import fs2.util._
import scodec.{codecs, Codec, DecodeResult, Attempt, Err}
import scodec.stream.{encode, decode, StreamDecoder, StreamEncoder}
import scodec.bits.{ByteVector, BitVector}

package object flatjoin_fs2 {

//   val frames: StreamDecoder[ByteVector] = decode.once(codecs.int32)
// .flatMap { numBytes => decode.once(codecs.bytes(numBytes)).isolate(numBytes) }
// .many

  val frameCodec: Codec[ByteVector] =
    codecs.variableSizeBytes(codecs.int32, codecs.bytes)

  def serialize[F[_], T: Format]: Pipe[F, T, ByteVector] =
    _.map(t => ByteVector(implicitly[Format[T]].toBytes(t)))

  def deserialize[F[_], T: Format]: Pipe[F, ByteVector, T] =
    _.map(b => implicitly[Format[T]].fromBytes(b.toByteBuffer))

  def dump[F[_]: Suspendable, T: Format]: Pipe[F, T, File] =
    serialize andThen writeFrames

  def writeFrames[F[_]: Suspendable]: Pipe[F, ByteVector, File] = { frames =>
    val streamEncoder: StreamEncoder[ByteVector] =
      scodec.stream.encode.many(frameCodec)

    val encodedFrames: Stream[F, ByteVector] =
      streamEncoder.encode(frames).map(_.toByteVector)

    val tmp = File.createTempFile("sort", "sort")
    tmp.deleteOnExit

    val dumped = encodedFrames
      .flatMap(byteVector => Stream.chunk(Chunk.bytes(byteVector.toArray)))
      .to(fs2.io.file.writeAll[F](tmp.toPath))
      .drain

    dumped ++ Stream(tmp)

  }

  def decode[F[_], T](codec: Codec[T]): Pipe[F, Byte, T] = {
    def loop(atLeast: Int): Handle[F, Byte] => Pull[F, T, Unit] =
      _.awaitN(atLeast).flatMap {
        case (chunks, nextHandle) =>
          val bv = chunks
            .map(chunk => BitVector(chunk.toBytes.values))
            .foldLeft(BitVector.empty)(_ ++ _)

          def decodeLoop(bv: BitVector): Pull[F, T, (Int, BitVector)] =
            codec.decode(bv) match {
              case Attempt.Successful(DecodeResult(t, remainder)) =>
                Pull.output1(t).map(_ => (0, remainder)) >> decodeLoop(
                  remainder)
              case Attempt.Failure(
                  Err.InsufficientBits(needed, has, context)) =>
                Pull.pure((needed.toInt - has.toInt) / 8 -> bv)
              case Attempt.Failure(err) =>
                Pull.fail(new RuntimeException(err.message))
            }

          decodeLoop(bv).flatMap {
            case (0, _) => throw new RuntimeException("should not happen")
            case (needed, remainder) =>
              loop(needed + (bv.size / 8).toInt)(
                nextHandle.push(Chunk.bytes(remainder.toByteArray)))
          }

      }

    _.pull(loop(0))
  }

  def readFrames[F[_]: Suspendable, T: Format]: Pipe[F, File, T] =
    _.flatMap(
      file =>
        fs2.io.file
          .readAll[F](file.toPath, 8192)
          .through(decode(frameCodec))
          .through(deserialize[F, T])
          .onFinalize(implicitly[Suspendable[F]].delay {
            file.delete; ()

          }))

  /* this fails */
  def mergeSorted2[F[_], T: Ordering]: Pipe2[F, T, T, T] = { (s1, s2) =>
    val ord = implicitly[Ordering[T]]
    def goB(h1: Handle[F, T], h2: Handle[F, T]): Pull[F, T, Unit] =
      h1.receive1Option {
        case Some((t1, h1)) =>
          h2.receive1Option {
            case Some((t2, h2)) if ord.lteq(t1, t2) =>
              Pull.output1(t1) >> goB(h1, h2.push1(t2))
            case Some((t2, h2)) =>
              Pull.output1(t2) >> goB(h1.push1(t1), h2)
            case None =>
              h1.push1(t1).echo
          }
        case None =>
          h2.echo
      }

    s1.pull2(s2)((x, y) => goB(x, y))
  }

  def sortInBatches[F[_]: Suspendable, T: Ordering: Format](
      max: Int): Pipe[F, T, File] =
    _.through(pipe.vectorChunkN(max)).map { x =>
      println(x); x
    }.flatMap(group => Stream(group.sorted: _*).through(dump))

  def mergeFiles[F[_]: Suspendable, T: Ordering: Format]: Pipe[F, File, T] =
    _.map(f => Stream(f).through(readFrames[F, T]))
      .reduce((x, y) => mergeSorted2.apply(x, y))
      .flatMap(identity)
      .map { x =>
        println(x); x
      }

  def sort[F[_]: Suspendable, T: Ordering: Format](max: Int): Pipe[F, T, T] =
    _.through(sortInBatches(max)).map { x =>
      println(x); x
    }.through(mergeFiles)

  def adjacentSpan[F[_], I](eq: (I, I) => Boolean): Pipe[F, I, Vector[I]] = {
    def go(buffer: Vector[I],
           last1: Option[I]): Handle[F, I] => Pull[F, Vector[I], Unit] = {
      _.receiveOption {
        case Some((chunk, h)) =>
          val (out, buf, last) = {
            chunk.foldLeft((List[Vector[I]](), buffer, last1)) {
              case ((out, buf, last), i) =>
                val sameAsLast = last.map(eq(_, i)).getOrElse(true)
                if (!sameAsLast)
                  (out :+ buf, Vector(i), Some(i))
                else (out, buf :+ i, Some(i))
            }
          }
          Pull.outputs(Stream(out: _*)) >> go(buf, last)(h)

        case None =>
          Pull.output1(buffer)
      }
    }
    _.pull { h =>
      go(Vector.empty, None)(h)
    }
  }

  def outerJoinSorted[F[_], T](columns: Int)(
      implicit ord: Ordering[(Int, T)]): Pipe[F, (Int, T), Seq[Option[T]]] =
    _.through(adjacentSpan((x, y) =>
      implicitly[Ordering[(Int, T)]].equiv(x, y))).flatMap(group =>
      Stream(flatjoin.crossGroup(group, columns).toList: _*))

  def sortAndOuterJoin[F[_]: Suspendable, T: Ordering](max: Int, columns: Int)(
      implicit f: Format[(Int, T)]): Pipe[F, (Int, T), Seq[Option[T]]] = {
    implicit val ordering: Ordering[(Int, T)] = Ordering.by(_._2)
    _.through(sort[F, (Int, T)](max)).through(outerJoinSorted(columns))
  }

  def concatStreamWithIndex[F[_], T](
      sources: Seq[Stream[F, T]]): Stream[F, (Int, T)] =
    sources.zipWithIndex.map {
      case (s, i) =>
        s.map(t => (i, t))
    }.reduce(_ ++ _)

}
