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

  def dump[T: Format](implicit ec: ExecutionContext): Sink[T, Future[File]] =
    Flow[T].map { elem =>
      ByteString(implicitly[Format[T]].toBytes(elem))
    }.toMat(dumpBytes)(Keep.right)

  def dumpBytes(
      implicit ec: ExecutionContext): Sink[ByteString, Future[File]] = {
    val tmp = File.createTempFile("sort", "sort")
    tmp.deleteOnExit

    Flow[ByteString]
      .via(Framing.simpleFramingProtocolEncoder(
        maximumMessageLength = 512 * 1024))
      .batchWeighted(512 * 1024, _.size, identity)(_ ++ _)
      .toMat(FileIO.toPath(tmp.toPath))(Keep.right)
      .mapMaterializedValue(x => x.filter(_.wasSuccessful).map(_ => tmp))
  }

  def sortInBatches[T: StringKey](max: Int)(
      implicit am: Materializer,
      sk: StringKey[T],
      fmt: Format[T]): Flow[T, File, NotUsed] = {
    import am.executionContext
    Flow[T].map { t =>
      val key = sk.key(t)
      (key, fmt.toBytes(t))
    }.batchWeighted(max, _._2.remaining, x => List(x))((x, y) => y :: x)
      .mapAsync(1) { group =>
        val sorted = group.sortBy(_._1)
        Source(sorted.map(x => ByteString(x._2))).runWith(dumpBytes)
      }
  }

  def readFileThenDelete[T: Format](file: File)(
      implicit ec: ExecutionContext): Source[T, Future[IOResult]] =
    FileIO
      .fromPath(file.toPath)
      .via(
        Framing.simpleFramingProtocolDecoder(maximumMessageLength = 512 * 1024)
      )
      .map { byteString =>
        implicitly[Format[T]].fromBytes(byteString.asByteBuffer)
      }
      .watchTermination()((old, future) => {
        future.onComplete {
          case _ =>
            file.delete
        }
        old
      })

  def merge[T: StringKey: Format](
      implicit am: Materializer): Flow[File, T, _] = {
    import am.executionContext
    implicit val ordering: Ordering[T] =
      Ordering.by(t => implicitly[StringKey[T]].key(t))
    def mergeFiles1(s: Seq[File]): Source[T, _] =
      s.map(f => readFileThenDelete[T](f))
        .reduce((s1, s2) => s1.mergeSorted(s2))

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

    val flows: List[Flow[(ByteString, Int), Seq[File], _]] = files.map {
      case (idx, file) =>
        sinkToFlow(
          Flow[(ByteString, Int)]
            .filter(_._2 == idx) //todo
            .map(_._1)
            .via(Framing.simpleFramingProtocolEncoder(
              maximumMessageLength = 512 * 1024))
            .toMat(FileIO.toPath(file.toPath))(Keep.right)
            .mapMaterializedValue(_.map(_ => file :: Nil))).mapAsync(1)(x => x)

    }.toList

    val shardFlow: Flow[(ByteString, Int), Seq[File], NotUsed] = Flow
      .fromGraph(
        GraphDSL.create() { implicit b =>
          import GraphDSL.Implicits._
          val broadcast = b.add(Broadcast[(ByteString, Int)](flows.size))
          val merge = b.add(Merge[Seq[File]](flows.size))
          flows.zipWithIndex.foreach {
            case (f, i) =>
              val flowShape = b.add(f)
              broadcast.out(i) ~> flowShape.in
              flowShape.out ~> merge.in(i)
          }

          new FlowShape(broadcast.in, merge.out)
        }
      )
      .reduce(_ ++ _)

    Flow[T].map {
      case elem =>
        ByteString(implicitly[Format[T]].toBytes(elem)) -> hash(elem)
    }.viaMat(shardFlow)(Keep.right)
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

          setHandler(in, new InHandler {
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
          })
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
      mat: Materializer): Flow[T, Seq[T], NotUsed] = {
    import mat.executionContext
    Flow[T].via(sort[T](max)).via(adjacentSpan)
  }

  def groupShardsBySorting[T: StringKey](max: Int)(
      implicit f: Format[T],
      mat: Materializer): Flow[File, Seq[T], NotUsed] = {
    import mat.executionContext
    Flow[File].flatMapConcat { file =>
      readFileThenDelete[T](file).via(sortAndGroup[T](max))
    }
  }

  def groupShardsByHashMap[T: StringKey](
      implicit f: Format[T],
      mat: Materializer): Flow[File, Seq[T], NotUsed] = {
    import mat.executionContext

    Flow[File].flatMapConcat { file =>
      readFileThenDelete[T](file).via(groupWithHashMap[T])
    }
  }

  def groupBySortingShards[T: StringKey](max: Int, parallelism: Int)(
      implicit f: Format[T],
      mat: Materializer): Flow[T, Seq[T], NotUsed] = {
    import mat.executionContext

    shard[T].flatMapConcat { (shards: Seq[File]) =>
      val (nonempty, empty) = shards.toList.partition(_.length > 0)
      empty.foreach(_.delete)
      Source(nonempty.toList).via(
        balancerUnordered(groupShardsBySorting(max), parallelism))

    }.mapMaterializedValue(_ => NotUsed)

  }

  def groupByShardsInMemory[T: StringKey](max: Int, parallelism: Int)(
      implicit f: Format[T],
      mat: Materializer): Flow[T, Seq[T], NotUsed] = {
    import mat.executionContext

    shard[T].flatMapConcat { (shards: Seq[File]) =>
      val (nonempty, empty) = shards.toList.partition(_.length > 0)
      empty.foreach(_.delete)
      Source(nonempty.toList).via(
        balancerUnordered(groupShardsByHashMap, parallelism))

    }.mapMaterializedValue(_ => NotUsed)

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

          setHandler(in, new InHandler {
            override def onUpstreamFinish(): Unit = {
              emitMultiple(out, mmap.values.toList)
              complete(out)
            }
            override def onPush(): Unit = {
              val elem = grab(in)

              val key = implicitly[StringKey[T]].key(elem)
              mmap.get(key) match {
                case None => mmap.update(key, ArrayBuffer(elem))
                case Some(buffer) => buffer.append(elem)
              }
              pull(in)
            }
          })
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

  def outerJoinByShards[T: StringKey](max: Int,
                                      columns: Int,
                                      parallelism: Int)(
      implicit f: Format[(Int, T)],
      mat: Materializer): Flow[(Int, T), Seq[Option[T]], NotUsed] = {
    implicit val sk = new StringKey[(Int, T)] {
      def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
    }
    Flow[(Int, T)]
      .via(groupByShardsInMemory[(Int, T)](max, parallelism))
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
    sources.zipWithIndex.map {
      case (s, i) =>
        s.map(t => (i, t))
    }.reduce(_ ++ _)

}
