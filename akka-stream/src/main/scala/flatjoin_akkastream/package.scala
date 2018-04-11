import akka._
import akka.util.ByteString
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import scala.concurrent._
import flatjoin._
import java.io.File
import java.nio._

package object flatjoin_akka {

  val maximumMessageLength = 1024 * 1024 * 10

  def balancerUnordered[In, Out](worker: Flow[In, Out, Any],
                                 workerCount: Int): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer =
        b.add(Balance[In](workerCount, waitForAllDownstreams = true))
      val merge = b.add(Merge[Out](workerCount))
      for (_ <- 1 to workerCount) {
        balancer ~> worker.async ~> merge
      }

      FlowShape.of(balancer.in, merge.out)
    })
  }

  def dump[T: Format: StringKey](
      implicit ec: ExecutionContext): Sink[T, Future[File]] =
    Flow[T]
      .map { elem =>
        implicitly[StringKey[T]].key(elem) -> ByteString(
          implicitly[Format[T]].toBytes(elem))
      }
      .toMat(dumpBytes)(Keep.right)

  def dumpBytes(implicit ec: ExecutionContext)
    : Sink[(String, ByteString), Future[File]] = {
    val tmp = File.createTempFile("sort", "sort")
    tmp.deleteOnExit

    Flow[(String, ByteString)]
      .mapConcat { case (key, bytes) => List(ByteString(key), bytes) }
      .via(Framing.simpleFramingProtocolEncoder(
        maximumMessageLength = maximumMessageLength))
      .batchWeighted(512 * 1024, _.size, identity)(_ ++ _)
      .toMat(FileIO.toPath(tmp.toPath))(Keep.right)
      .mapMaterializedValue(x => x.filter(_.wasSuccessful).map(_ => tmp))
  }

  def sortInBatches[T: StringKey](max: Int)(
      implicit am: Materializer,
      sk: StringKey[T],
      fmt: Format[T]): Flow[T, File, NotUsed] = {
    import am.executionContext
    Flow[T]
      .map { t =>
        val key = sk.key(t)
        (key, fmt.toBytes(t))
      }
      .grouped(max)
      .mapAsync(1) { group =>
        val sorted = group.sortBy(_._1)
        Source(sorted.map(x => x._1 -> ByteString(x._2))).runWith(dumpBytes)
      }
  }

  def sortInBatches2(max: Int)(
      implicit am: Materializer): Flow[(String, ByteString), File, NotUsed] = {
    import am.executionContext
    Flow[(String, ByteString)]
      .grouped(max)
      .mapAsync(1) { group =>
        val sorted = group.sortBy(_._1)
        Source(sorted.map(x => x._1 -> x._2)).runWith(dumpBytes)
      }
  }

  def readFileThenDelete(file: File)(implicit ec: ExecutionContext)
    : Source[(String, ByteString), Future[IOResult]] =
    FileIO
      .fromPath(file.toPath)
      .via(
        Framing.simpleFramingProtocolDecoder(
          maximumMessageLength = maximumMessageLength)
      )
      .grouped(2)
      .map {
        case Seq(key, data) =>
          key.utf8String -> data
      }
      .watchTermination()((old, future) => {
        future.onComplete {
          case _ =>
            file.delete
        }
        old
      })

  def deserialize[T: Format](
      implicit am: Materializer): Flow[(String, ByteString), T, _] = {
    import am.executionContext
    Flow[(String, ByteString)]
      .map {
        case (_, data) =>
          implicitly[Format[T]].fromBytes(data.asByteBuffer)
      }
  }

  def merge[T: StringKey: Format](
      implicit am: Materializer): Flow[File, T, _] = {
    import am.executionContext
    implicit val ordering: Ordering[(String, ByteString)] =
      Ordering.by(_._1)
    def mergeFiles1(s: Seq[File]): Source[T, _] =
      s.map(f => readFileThenDelete(f))
        .reduce((s1, s2) => s1.mergeSorted(s2))
        .map {
          case (key, data) =>
            implicitly[Format[T]].fromBytes(data.asByteBuffer)
        }

    def merge(s: Seq[File]): Source[T, _] =
      if (s.size < 50) {
        mergeFiles1(s)
      } else
        Source
          .fromFuture(
            Future.sequence(
              s.grouped(50)
                .map { g =>
                  mergeFiles1(g).runWith(dump)
                }
                .toList))
          .flatMapConcat(s => merge(s))

    Flow[File].grouped(Int.MaxValue).flatMapConcat(s => merge(s))
  }

  def sinkToFlow[T, U](sink: Sink[T, U]): Flow[T, U, U] =
    Flow.fromGraph(GraphDSL.create(sink) { implicit builder => sink =>
      FlowShape.of(sink.in, builder.materializedValue)
    })

  def shard[T: Format: StringKey](
      implicit ec: ExecutionContext): Flow[T, Seq[File], NotUsed] = {

    val files = 0 until 128 map { i =>
      val file = File.createTempFile("shard_" + i + "_", "shard")
      file.deleteOnExit
      (i, file)
    } toMap

    val hash = (t: T) =>
      math.abs(
        scala.util.hashing.MurmurHash3
          .stringHash(implicitly[StringKey[T]].key(t)) % 128)

    val flows: List[(Int, Flow[(String, ByteString, Int), Seq[File], _])] =
      files.map {
        case (idx, file) =>
          idx -> sinkToFlow(
            Flow[(String, ByteString, Int)]
              .mapConcat { case (key, data, _) => List(ByteString(key), data) }
              .via(Framing.simpleFramingProtocolEncoder(maximumMessageLength =
                maximumMessageLength))
              .batchWeighted(512 * 1024, _.size, identity)(_ ++ _)
              .toMat(FileIO.toPath(file.toPath))(Keep.right)
              .mapMaterializedValue(_.map(_ => file :: Nil))).mapAsync(1)(x =>
            x)

      }.toList

    val shardFlow: Flow[(String, ByteString, Int), Seq[File], NotUsed] = Flow
      .fromGraph(
        GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val broadcast =
            b.add(Partition[(String, ByteString, Int)](flows.size, _._3))
          val merge = b.add(Merge[Seq[File]](flows.size))
          flows.foreach {
            case (i, f) =>
              val flowShape = b.add(f)
              broadcast.out(i) ~> flowShape.in
              flowShape.out ~> merge.in(i)
          }

          new FlowShape(broadcast.in, merge.out)
        }
      )
      .reduce(_ ++ _)

    Flow[T]
      .map {
        case elem =>
          (implicitly[StringKey[T]].key(elem),
           ByteString(implicitly[Format[T]].toBytes(elem)),
           hash(elem))
      }
      .viaMat(shardFlow)(Keep.right)
  }

  def adjacentSpan[T: StringKey]: Flow[T, Seq[T], NotUsed] =
    Flow.fromGraph(new GraphStage[FlowShape[T, Seq[T]]] {
      val in: Inlet[T] = Inlet("adjacentSpanIn")
      val out: Outlet[Seq[T]] = Outlet("adjacentSpanOut")

      val sk = implicitly[StringKey[T]]

      override val shape = FlowShape.of(in, out)

      override def createLogic(attr: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {

          val buffer = scala.collection.mutable.ArrayBuffer[T]()

          setHandler(
            in,
            new InHandler {
              override def onUpstreamFinish(): Unit = {
                emit(out, buffer.toList)
                complete(out)
              }
              override def onPush(): Unit = {
                val elem = grab(in)
                if (buffer.isEmpty || sk.key(buffer.head) == sk.key(elem)) {
                  buffer.append(elem)
                  pull(in)
                } else {
                  val k = buffer.toList
                  buffer.clear
                  buffer.append(elem)
                  push(out, k)
                }
              }
            }
          )
          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              pull(in)
            }
          })
        }
    })

  def sort[T: StringKey: Format](max: Int)(
      implicit am: Materializer): Flow[T, T, NotUsed] = {
    import am.executionContext
    sortInBatches[T](max) via merge[T]
  }

  def sortAndGroup[T: StringKey](max: Int)(
      implicit f: Format[T],
      mat: Materializer): Flow[(String, ByteString), Seq[T], NotUsed] = {
    import mat.executionContext
    sortInBatches2(max) via merge[T] via adjacentSpan
  }

  def groupShardsBySorting[T: StringKey](max: Int)(
      implicit f: Format[T],
      mat: Materializer): Flow[File, Seq[T], NotUsed] = {
    import mat.executionContext
    Flow[File].flatMapConcat { file =>
      readFileThenDelete(file).via(sortAndGroup[T](max))
    }
  }

  def groupShardsByHashMap[T: StringKey](
      implicit f: Format[T],
      mat: Materializer): Flow[File, Seq[T], NotUsed] = {
    import mat.executionContext

    Flow[File].flatMapConcat { file =>
      readFileThenDelete(file).via(deserialize[T]).via(groupWithHashMap[T])
    }
  }

  def groupBySortingShards[T: StringKey](max: Int, parallelism: Int)(
      implicit f: Format[T],
      mat: Materializer): Flow[T, Seq[T], NotUsed] = {
    import mat.executionContext

    shard[T]
      .flatMapConcat { (shards: Seq[File]) =>
        val (nonempty, empty) = shards.toList.partition(_.length > 0)
        empty.foreach(_.delete)
        Source(nonempty.toList).via(
          balancerUnordered(groupShardsBySorting(max), parallelism))

      }
      .mapMaterializedValue(_ => NotUsed)

  }

  def groupByShardsInMemory[T: StringKey](parallelism: Int)(
      implicit f: Format[T],
      mat: Materializer): Flow[T, Seq[T], NotUsed] = {
    import mat.executionContext

    shard[T]
      .flatMapConcat { (shards: Seq[File]) =>
        val (nonempty, empty) = shards.toList.partition(_.length > 0)
        empty.foreach(_.delete)
        Source(nonempty.toList).via(
          balancerUnordered(groupShardsByHashMap, parallelism))

      }
      .mapMaterializedValue(_ => NotUsed)

  }

  def groupWithHashMap[T: StringKey]: Flow[T, Seq[T], NotUsed] =
    Flow.fromGraph(new GraphStage[FlowShape[T, Seq[T]]] {
      val in: Inlet[T] = Inlet("groupInMemory.in")
      val out: Outlet[Seq[T]] = Outlet("groupInMemory.out")

      import scala.collection.mutable.{ArrayBuffer, AnyRefMap}

      override val shape = FlowShape.of(in, out)

      override def createLogic(attr: Attributes): GraphStageLogic =
        new GraphStageLogic(shape) {

          val mmap =
            AnyRefMap[String, ArrayBuffer[T]]()

          setHandler(
            in,
            new InHandler {
              override def onUpstreamFinish(): Unit = {
                emitMultiple(out, mmap.values.toList)
                complete(out)
              }
              override def onPush(): Unit = {
                val elem = grab(in)

                val key = implicitly[StringKey[T]].key(elem)
                mmap.get(key) match {
                  case None         => mmap.update(key, ArrayBuffer(elem))
                  case Some(buffer) => buffer.append(elem)
                }
                pull(in)
              }
            }
          )
          setHandler(out, new OutHandler {
            override def onPull(): Unit = {
              pull(in)
            }
          })
        }
    })

  def outerJoinGrouped[T](
      columns: Int): Flow[Seq[(Int, T)], Seq[Option[T]], NotUsed] =
    Flow[Seq[(Int, T)]].mapConcat { group =>
      crossGroup(group, columns).toList
    }

  def outerJoinInMemory[T: StringKey](
      columns: Int): Flow[(Int, T), Seq[Option[T]], NotUsed] = {
    implicit val sk = new StringKey[(Int, T)] {
      def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
    }

    Flow[(Int, T)]
      .via(groupWithHashMap[(Int, T)])
      .via(outerJoinGrouped(columns))
  }

  def outerJoinByShards[T: StringKey](columns: Int, parallelism: Int)(
      implicit f: Format[(Int, T)],
      mat: Materializer): Flow[(Int, T), Seq[Option[T]], NotUsed] = {
    implicit val sk = new StringKey[(Int, T)] {
      def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
    }
    Flow[(Int, T)]
      .via(groupByShardsInMemory[(Int, T)](parallelism))
      .via(outerJoinGrouped(columns))
  }

  def outerJoinSorted[T: StringKey](
      columns: Int): Flow[(Int, T), Seq[Option[T]], NotUsed] = {
    implicit val sk = new StringKey[(Int, T)] {
      def key(i: (Int, T)) = implicitly[StringKey[T]].key(i._2)
    }
    Flow[(Int, T)].via(adjacentSpan).via(outerJoinGrouped(columns))
  }

  def sortAndOuterJoin[T: StringKey](max: Int, columns: Int)(
      implicit f: Format[(Int, T)],
      mat: Materializer): Flow[(Int, T), Seq[Option[T]], NotUsed] = {
    import mat.executionContext
    implicit val sk = new StringKey[(Int, T)] {
      def key(i: (Int, T)) = implicitly[StringKey[T]].key(i._2)
    }
    Flow[(Int, T)].via(sort[(Int, T)](max)).via(outerJoinSorted(columns))
  }

  def outerJoinBySortingShards[T: StringKey](max: Int,
                                             columns: Int,
                                             parallelism: Int)(
      implicit f: Format[(Int, T)],
      mat: Materializer): Flow[(Int, T), Seq[Option[T]], NotUsed] = {
    implicit val sk = new StringKey[(Int, T)] {
      def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
    }
    Flow[(Int, T)]
      .via(groupBySortingShards[(Int, T)](max, parallelism))
      .via(outerJoinGrouped(columns))
  }

  def concatSources[T](sources: Seq[Source[T, _]]): Source[(Int, T), _] =
    sources.zipWithIndex
      .map {
        case (s, i) =>
          s.map(t => (i, t))
      }
      .reduce(_ ++ _)

}
