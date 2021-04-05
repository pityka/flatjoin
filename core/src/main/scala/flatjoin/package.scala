import scala.collection.mutable.ArrayBuffer

package object flatjoin {
  def crossGroup[T](
      group: Seq[(Int, T)],
      columns: Int
  ): Iterator[Seq[Option[T]]] =
    cross(group.groupBy(_._1).toSeq.map(_._2.toList).toList).iterator.map {
      group =>
        val s = ArrayBuffer.fill[Option[T]](columns)(None)
        group.foreach { case (i, t) =>
          s(i) = Some(t)
        }
        s.toSeq
    }

  def cross[T](list: List[List[T]]): List[List[T]] =
    list match {
      case xs :: Nil => xs map (x => x :: Nil)
      case x :: xs =>
        for {
          i <- x
          j <- cross(xs)
        } yield List(i) ++ j
      case Nil => throw new RuntimeException("should not happen")
    }
}
