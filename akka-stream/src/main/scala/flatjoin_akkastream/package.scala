import akka._
import akka.util.ByteString
import akka.actor._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import scala.concurrent._
import scala.util.{Success, Failure}
import scala.concurrent.duration._
import flatjoin._
import java.io.File
import java.nio._
import com.typesafe.scalalogging.Logger
import scala.language.postfixOps

package object flatjoin_akka {

  def concatByteStrings(bs: Traversable[ByteString]) = {
    val totalSize = bs.foldLeft(0)((a, b) => a + b.length)
    val dst = java.nio.ByteBuffer.allocate(totalSize)

    bs.foreach { byteString =>
      byteString.asByteBuffers.foreach { source =>
        dst.put(source)
      }
    }
    dst.rewind
    ByteString(dst)
  }

  def concatSources[T](sources: Seq[Source[T, _]]): Source[(Int, T), _] =
    sources.zipWithIndex
      .map {
        case (s, i) =>
          s.map(t => (i, t))
      }
      .reduce(_ ++ _)

  def parallelize[T, K](parallelism: Int, bufferSize: Int = 1000)(f: T => K)(
      implicit
      ec: ExecutionContext
  ): Flow[T, K, _] =
    if (parallelism == 1)
      Flow[T].map(f)
    else
      Flow[T]
        .grouped(bufferSize)
        .via(
          (Flow[scala.collection.immutable.Iterable[T]]
            .mapAsync(parallelism) { lines =>
              Future {
                lines.map(f)
              }(ec)
            })
            .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
        )
        .mapConcat(identity)

  def balancerUnordered[In, Out](
      worker: Flow[In, Out, Any],
      workerCount: Int,
      groupSize: Int
  ): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b =>
      val balancer =
        b.add(Balance[In](workerCount, waitForAllDownstreams = true))
      val merge = b.add(Merge[Out](workerCount))
      for (_ <- 1 to workerCount) {
        balancer ~> (worker
          .grouped(groupSize))
          .async ~> Flow[Seq[Out]]
          .mapConcat(_.toList) ~> merge
      }

      FlowShape.of(balancer.in, merge.out)
    })
  }

  def deserialize[T: Format](
      implicit am: Materializer
  ): Flow[ByteString, T, _] = {
    import am.executionContext
    Flow[ByteString]
      .map { data =>
        implicitly[Format[T]].fromBytes(data.asByteBuffer)
      }
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

  def sinkToFlow[T, U](sink: Sink[T, U]): Flow[T, U, U] =
    Flow.fromGraph(GraphDSL.create(sink) { implicit builder => sink =>
      FlowShape.of(sink.in, builder.materializedValue)
    })

  def tempFileSink(implicit ec: ExecutionContext) =
    Sink
      .lazyInitAsync { () =>
        val tmp = File.createTempFile("flat", ".dump")
        tmp.deleteOnExit

        Future.successful(
          FileIO
            .toPath(tmp.toPath)
            .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
            .mapMaterializedValue(
              _.transform {
                case Success(ioResult) if ioResult.status.isSuccess =>
                  Success((tmp, ioResult.count))
                case Success(ioResult) =>
                  logger.error(
                    s"temp file sink failed. ${ioResult.status}",
                    ioResult.status.failed.get
                  )
                  Failure(ioResult.status.failed.get)
                case fail => Failure(fail.failed.get)
              }
            )
        )

      }
      .mapMaterializedValue(
        futureOptionFutureFile =>
          futureOptionFutureFile.flatMap {
            case Some(f) => f.map(Some(_))
            case None    => Future.successful(None)
          }
      )

  def tempFile(implicit ec: ExecutionContext): Flow[ByteString, File, _] =
    sinkToFlow(tempFileSink)
      .mapAsync(1)(identity)
      .filter(_.isDefined)
      .map(_.get)
      .map { case (file, _) => file }

  private[this] val logger = Logger("flatjoin")

  case class Instance(
      maximumMessageLength: Int = 1024 * 1024 * 1800,
      writeBufferSize: Long = 1024 * 1024 * 10L,
      mergeReadBufferSize: Int = 1024 * 1024 * 10,
      parallelMerges: Int = 1,
      mergeMaxOpenFiles: Int = 50,
      compressionLevel: Int = 1,
      numberOfShards: Int = 128,
      numberOfBatchesSortedInParallel: Int = 1,
      parallelismOfShardComputation: Int = 1,
      numberOfBucketsSortedInParallel: Int = 1,
      numberOfShardsJoinedInParallel: Int = 1
  ) {

    def dump[T: Format: StringKey](
        implicit ec: ExecutionContext
    ): Sink[T, Future[Option[File]]] =
      Flow[T]
        .map { elem =>
          implicitly[StringKey[T]].key(elem) -> ByteString(
            implicitly[Format[T]].toBytes(elem)
          )
        }
        .toMat(dumpBytes.mapMaterializedValue(_.map(_.map(_._1))))(Keep.right)

    def dumpBytes(
        implicit ec: ExecutionContext
    ): Sink[(String, ByteString), Future[Option[(File, Long)]]] = {

      val fileSink = tempFileSink

      Flow[(String, ByteString)]
        .mapConcat {
          case (key, bytes) => ByteString(key) :: bytes :: Nil
        }
        .via(
          Framing.simpleFramingProtocolEncoder(
            maximumMessageLength = maximumMessageLength
          )
        )
        .groupedWeightedWithin(mergeReadBufferSize, 2 seconds)(_.size)
        .map(concatByteStrings)
        .toMat(fileSink)(Keep.right)
    }

    def sortInBatches[T: StringKey](
        implicit am: Materializer,
        sk: StringKey[T],
        fmt: Format[T]
    ): Flow[T, File, NotUsed] = {
      import am.executionContext
      Flow[T]
        .map { t =>
          val key = sk.key(t)
          (key, fmt.toBytes(t))
        }
        .groupedWeightedWithin(writeBufferSize, 2 seconds)(_._2.remaining)
        .map(_.toArray)
        .via(
          (Flow[Array[(String, ByteBuffer)]]
            .mapAsync(numberOfBatchesSortedInParallel) { group =>
              scala.util.Sorting.quickSort(group)(Ordering.by(_._1))
              Source
                .fromIterator(
                  () => group.iterator.map(x => x._1 -> ByteString(x._2))
                )
                .runWith(dumpBytes.mapMaterializedValue(_.map(_.map(_._1))))
            })
            .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
        )
        .filter(_.isDefined)
        .map(_.get)
    }

    def sortInBatches2(
        implicit am: Materializer
    ): Flow[(String, ByteString), File, NotUsed] = {
      import am.executionContext
      Flow[(String, ByteString)]
        .groupedWeightedWithin(writeBufferSize, 2 seconds)(_._2.size)
        .map(_.toArray)
        .via(
          (Flow[Array[(String, ByteString)]]
            .mapAsync(numberOfBatchesSortedInParallel) { group =>
              scala.util.Sorting.quickSort(group)(Ordering.by(_._1))
              Source
                .fromIterator(() => group.iterator.map(x => x._1 -> x._2))
                .runWith(dumpBytes.mapMaterializedValue(_.map(_.map(_._1))))
            })
            .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
        )
        .collect { case Some(file) => file }
    }

    def readFileThenDelete(file: File, bufferSize: Long)(
        implicit ec: ExecutionContext
    ): Source[(String, ByteString), Future[IOResult]] =
      FileIO
        .fromPath(file.toPath)
        .groupedWeightedWithin(bufferSize, 2 seconds)(_.size)
        .mapConcat(identity)
        .via(
          Framing.simpleFramingProtocolDecoder(
            maximumMessageLength = maximumMessageLength
          )
        )
        .grouped(2)
        .map {
          case Seq(key, data) =>
            key.utf8String -> data
        }
        .watchTermination()((old, future) => {
          future.onComplete {
            case Success(_) =>
              logger.debug("Deleting temporary file $file.")
              file.delete
            case Failure(e) =>
              logger.error(s"Failed reading back temporary file: $file", e)
          }
          old
        })

    def merge[T: StringKey: Format](
        makeSource: File => Source[(String, ByteString), _]
    )(implicit am: Materializer): Flow[File, T, _] = {
      import am.executionContext
      implicit val ordering: Ordering[(String, ByteString)] =
        Ordering.by(_._1)
      def mergeFiles1(
          s: Seq[File]
      )(makeSource: File => Source[(String, ByteString), _]): Source[T, _] =
        s.map(f => makeSource(f))
          .reduce((s1, s2) => s1.mergeSorted(s2))
          .map {
            case (key, data) =>
              implicitly[Format[T]].fromBytes(data.asByteBuffer)
          }

      def merge(
          s: Seq[File]
      )(makeSource: File => Source[(String, ByteString), _]): Source[T, _] = {
        logger.debug("Merging " + s.size + " files")

        if (s.size < mergeMaxOpenFiles) {
          mergeFiles1(s)(makeSource)
        } else
          Source(s.grouped(mergeMaxOpenFiles).toList)
            .mapAsync(parallelMerges) { g =>
              mergeFiles1(g)(makeSource).runWith(dump)
            }
            .grouped(Int.MaxValue)
            .flatMapConcat(
              s =>
                merge(s.flatten)(
                  readFileThenDelete(_, bufferSize = mergeReadBufferSize)
                )
            )
      }

      Flow[File].grouped(Int.MaxValue).flatMapConcat(s => merge(s)(makeSource))
    }

    def bucket[T: Format: StringKey](
        toBucket: String => String
    )(implicit ec: ExecutionContext): Flow[T, (File, Long), NotUsed] = {

      def bubble[In, Out1, Out2](
          flow1: Flow[In, Out1, _],
          flow2: Flow[In, Out2, _]
      ) =
        Flow.fromGraph(GraphDSL.create(flow1, flow2)((_, _)) {
          implicit b => (flow1, flow2) =>
            import GraphDSL.Implicits._

            val broadcast = b.add(Broadcast[In](2))
            val zip = b.add(Zip[Out1, Out2]())

            broadcast.out(0) ~> flow1 ~> zip.in0
            broadcast.out(1) ~> flow2 ~> zip.in1

            FlowShape(broadcast.in, zip.out)
        })

      def dumpToBucket: Flow[
        (String, List[(String, ByteString)]),
        ((File, Long), String),
        _
      ] = {
        val head = Flow[(String, List[(String, ByteString)])]
          .map(elem => elem._1)
          .take(1)
        val file =
          Flow[(String, List[(String, ByteString)])]
            .mapConcat(_._2)
            .via(sinkToFlow(dumpBytes).mapAsync(1)(identity))
            .collect { case Some(f) => f }

        bubble(file, head)

      }

      Flow[T]
        .map { t =>
          val key = implicitly[StringKey[T]].key(t)
          val data = ByteString(implicitly[Format[T]].toBytes(t))
          val buck = toBucket(key)
          (data, buck, key)
        }
        .groupedWeightedWithin(mergeReadBufferSize, 2 seconds)(_._1.length)
        .mapConcat { group =>
          group
            .groupBy(_._2)
            .iterator
            .map {
              case (bucket, group) =>
                (bucket, group.map(triple => triple._3 -> triple._1).toList)
            }
            .toList

        }
        .groupBy(Int.MaxValue, _._1)
        .via(dumpToBucket)
        .mergeSubstreams
        .grouped(Int.MaxValue)
        .mapConcat(buckets => {
          logger.debug("Created ${buckets.size} buckets.")
          buckets.sortBy(_._2).map(_._1)
        })

    }

    def shard[T: Format: StringKey](
        implicit ec: ExecutionContext
    ): Flow[T, Seq[(Int, File)], NotUsed] = {

      val hash = (t: T) =>
        math.abs(
          scala.util.hashing.MurmurHash3
            .stringHash(implicitly[StringKey[T]].key(t)) % numberOfShards
        )

      val shardFlow
          : Flow[(String, ByteString, Int), Seq[(Int, File)], NotUsed] =
        Flow
          .fromGraph(
            GraphDSL.create() { implicit b =>
              import GraphDSL.Implicits._

              val flows: List[
                (Int, Flow[(String, ByteString, Int), (Int, File), _])
              ] =
                (0 until numberOfShards).map {
                  idx =>
                    (
                      idx,
                      Flow[(String, ByteString, Int)]
                        .mapConcat {
                          case (key, data, _) => List(ByteString(key), data)
                        }
                        .via(
                          Framing.simpleFramingProtocolEncoder(
                            maximumMessageLength = maximumMessageLength
                          )
                        )
                        .groupedWeightedWithin(mergeReadBufferSize, 2 seconds)(
                          _.size
                        )
                        .map(concatByteStrings)
                        .via(tempFile)
                        .map(f => (idx, f))
                    )

                }.toList

              val broadcast =
                b.add(Partition[(String, ByteString, Int)](flows.size, _._3))
              val merge = b.add(Merge[(Int, File)](flows.size))
              flows.foreach {
                case (i, f) =>
                  val flowShape = b.add(f)
                  broadcast.out(i) ~> flowShape.in
                  flowShape.out ~> merge.in(i)
              }

              new FlowShape(broadcast.in, merge.out)
            }
          )
          .fold(List.empty[(Int, File)])(_ :+ _)

      parallelize[T, (String, ByteString, Int)](
        parallelismOfShardComputation,
        3000
      ) {
        case elem =>
          (
            implicitly[StringKey[T]].key(elem),
            ByteString(implicitly[Format[T]].toBytes(elem)),
            hash(elem)
          )
      }.viaMat(shardFlow)(Keep.right)
    }

    def sort[T: StringKey: Format](
        implicit am: Materializer
    ): Flow[T, T, NotUsed] = {
      import am.executionContext
      sortInBatches[T] via merge[T](
        readFileThenDelete(_, bufferSize = mergeReadBufferSize)
      )
    }

    def bucketSort[T: StringKey: Format](
        toBucket: String => String
    )(implicit am: Materializer): Flow[T, T, NotUsed] = {
      import am.executionContext

      def sortBucket(bucketFile: File, bucketFileSize: Long) = {
        if (bucketFileSize > writeBufferSize)
          readFileThenDelete(bucketFile, bufferSize = mergeReadBufferSize) via sortInBatches2 via merge(
            readFileThenDelete(_, bufferSize = mergeReadBufferSize)
          )
        else
          readFileThenDelete(bucketFile, bufferSize = mergeReadBufferSize)
            .grouped(Int.MaxValue)
            .map(_.toArray)
            .mapConcat { group =>
              scala.util.Sorting.quickSort(group)(Ordering.by(_._1))

              group.iterator
                .map(
                  data => implicitly[Format[T]].fromBytes(data._2.asByteBuffer)
                )
                .toVector
            }
      }

      if (numberOfBucketsSortedInParallel == 1)
        bucket(toBucket).flatMapConcat {
          case (bucket, bucketFileSize) =>
            sortBucket(bucket, bucketFileSize)
        } else
        bucket(toBucket)
          .mapAsync(numberOfBucketsSortedInParallel) {
            case (bucket, bucketFileSize) =>
              sortBucket(bucket, bucketFileSize).runWith(Sink.seq)
          }
          .mapConcat(identity)

    }

    def sortAndGroup[T: StringKey](
        implicit f: Format[T],
        mat: Materializer
    ): Flow[(String, ByteString), Seq[T], NotUsed] = {
      import mat.executionContext
      sortInBatches2 via merge[T](
        readFileThenDelete(_, bufferSize = mergeReadBufferSize)
      ) via adjacentSpan
    }

    def groupShardsBySorting[T: StringKey](
        implicit f: Format[T],
        mat: Materializer
    ): Flow[File, Seq[T], NotUsed] = {
      import mat.executionContext
      Flow[File].flatMapConcat { file =>
        readFileThenDelete(file, mergeReadBufferSize).via(sortAndGroup[T])
      }
    }

    def groupShardsByHashMap(
        implicit
        mat: Materializer
    ): Flow[Seq[(Int, File)], Seq[(Int, (String, ByteString))], NotUsed] = {
      import mat.executionContext
      implicit val sk2 = new StringKey[(Int, (String, ByteString))] {
        def key(a: (Int, (String, ByteString))) = a._2._1
      }
      Flow[Seq[(Int, File)]]
        .flatMapConcat {
          case list =>
            Source(list.toList)
              .flatMapConcat {
                case (idx, file) =>
                  readFileThenDelete(file, mergeReadBufferSize)
                    .map { case (key, data) => (idx, (key, data)) }

              }
              .via(groupWithHashMap[(Int, (String, ByteString))])
        }

    }

    def groupBySortingShards[T: StringKey](
        implicit f: Format[T],
        mat: Materializer
    ): Flow[T, Seq[T], NotUsed] = {
      import mat.executionContext

      shard[T]
        .flatMapConcat { (shards: Seq[(Int, File)]) =>
          Source(shards.toList.map(_._2))
            .via(
              balancerUnordered(
                groupShardsBySorting,
                numberOfShardsJoinedInParallel,
                groupSize = 256
              )
            )
            .watchTermination() {
              case (mat, future) =>
                future.onComplete {
                  case _ =>
                    shards.foreach(_._2.delete)
                }
                mat
            }

        }
        .mapMaterializedValue(_ => NotUsed)

    }

    def groupByShardsInMemory[T: StringKey](
        implicit f: Format[T],
        mat: Materializer
    ): Flow[T, Seq[T], NotUsed] = {
      Flow[T]
        .map((0, _))
        .via(groupByShardsInMemory(1, Nil))
        .map(_.map(_._2))
    }

    def groupByShardsInMemory[T: StringKey](
        maxGroups: Int,
        requireAllTheseColumns: List[Int] = Nil
    )(
        implicit f: Format[T],
        mat: Materializer
    ): Flow[(Int, T), Seq[(Int, T)], NotUsed] = {
      import mat.executionContext

      def first[T]: Flow[T, T, _] =
        sinkToFlow(Sink.head[T]).mapAsync(1)(identity)

      def byPass[T, K, R](flow: Flow[T, R, _]): Flow[(K, T), (K, R), _] =
        Flow
          .fromGraph(
            GraphDSL.create(flow) { implicit b => flow =>
              import GraphDSL.Implicits._
              val broadcast = b.add(Unzip[K, T])
              val zip = b.add(Zip[K, R]())
              val f = b.add(first[K])
              broadcast.out0 ~> f ~> zip.in0
              broadcast.out1 ~> flow ~> zip.in1

              new FlowShape(broadcast.in, zip.out)
            }
          )

      Flow[(Int, T)]
        .groupBy(maxSubstreams = maxGroups, _._1)
        .via(byPass(shard[T]))
        .mergeSubstreams
        .grouped(maxGroups)
        .flatMapConcat { groups =>
          val shards = groups
            .flatMap {
              case (indexOfGroup, shards) =>
                shards.map {
                  case (indexOfShard, f) =>
                    (indexOfGroup, indexOfShard, f)
                }
            }
            .groupBy(_._2)
            .toSeq
            .map {
              case (_, group) =>
                (group.map {
                  case (indexOfGroup, _, file) => (indexOfGroup, file)
                })
            }

          Source(shards.toList)
            .via(
              balancerUnordered(
                groupShardsByHashMap,
                numberOfShardsJoinedInParallel,
                groupSize = 1
              )
            )
            .map {
              case group =>
                val keepGroup = requireAllTheseColumns.isEmpty || requireAllTheseColumns
                  .forall(i => group.exists(_._1 == i))
                if (keepGroup)
                  group.map {
                    case (column, (_, data)) =>
                      (column, f.fromBytes(data.asByteBuffer))
                  } else Nil
            }
            .watchTermination() {
              case (mat, future) =>
                future.onComplete {
                  case _ =>
                    shards.foreach(_.foreach(_._2.delete))
                }
                mat
            }

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

            var mmap =
              AnyRefMap[String, ArrayBuffer[T]]()

            override def postStop() = {
              mmap = null
            }

            setHandler(
              in,
              new InHandler {
                override def onUpstreamFinish(): Unit = {
                  val iterator = mmap.iterator.map {
                    case (key, value) =>
                      mmap.remove(key)
                      value.toSeq
                  }
                  emitMultiple(out, iterator)
                  complete(out)
                }
                override def onPush(): Unit = {
                  val elem = grab(in)
                  val key = implicitly[StringKey[T]].key(elem)
                  mmap.get(key) match {
                    case None =>
                      mmap.update(key, ArrayBuffer(elem))
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
        columns: Int
    ): Flow[Seq[(Int, T)], Seq[Seq[Option[T]]], NotUsed] =
      Flow[Seq[(Int, T)]].filter(_.nonEmpty).map { group =>
        crossGroup(group, columns).toList
      }

    def outerJoinInMemory[T: StringKey](
        columns: Int
    ): Flow[(Int, T), Seq[Seq[Option[T]]], NotUsed] = {
      implicit val sk = new StringKey[(Int, T)] {
        def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
      }

      Flow[(Int, T)]
        .via(groupWithHashMap[(Int, T)])
        .via(outerJoinGrouped(columns))
    }

    def joinByShards[T: StringKey](
        columns: Int,
        requireAllTheseColumns: List[Int] = Nil
    )(
        implicit f: Format[T],
        mat: Materializer
    ): Flow[(Int, T), Seq[Seq[Option[T]]], NotUsed] = {
      implicit val sk = new StringKey[(Int, T)] {
        def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
      }
      Flow[(Int, T)]
        .via(
          groupByShardsInMemory[T](columns, requireAllTheseColumns)
        )
        .via(outerJoinGrouped(columns))
    }

    def outerJoinSorted[T: StringKey](
        columns: Int
    ): Flow[(Int, T), Seq[Seq[Option[T]]], NotUsed] = {
      implicit val sk = new StringKey[(Int, T)] {
        def key(i: (Int, T)) = implicitly[StringKey[T]].key(i._2)
      }
      Flow[(Int, T)].via(adjacentSpan).via(outerJoinGrouped(columns))
    }

    def sortAndOuterJoin[T: StringKey](columns: Int)(
        implicit f: Format[(Int, T)],
        mat: Materializer
    ): Flow[(Int, T), Seq[Seq[Option[T]]], NotUsed] = {
      import mat.executionContext
      implicit val sk = new StringKey[(Int, T)] {
        def key(i: (Int, T)) = implicitly[StringKey[T]].key(i._2)
      }
      Flow[(Int, T)].via(sort[(Int, T)]).via(outerJoinSorted(columns))
    }

    def outerJoinBySortingShards[T: StringKey](
        columns: Int
    )(
        implicit f: Format[(Int, T)],
        mat: Materializer
    ): Flow[(Int, T), Seq[Seq[Option[T]]], NotUsed] = {
      implicit val sk = new StringKey[(Int, T)] {
        def key(t: (Int, T)) = implicitly[StringKey[T]].key(t._2)
      }
      Flow[(Int, T)]
        .via(groupBySortingShards[(Int, T)])
        .via(outerJoinGrouped(columns))
    }

  }
}
