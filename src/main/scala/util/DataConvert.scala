package util

import scala.math.Ordering._

object DataConvert {

  def convert[A <% Ordering[A]](dataSet: IndexedSeq[IndexedSeq[A]]): IndexedSeq[(Double, IndexedSeq[Double])] = {
    def col(index: Int): IndexedSeq[A] = {
      dataSet.map(row => row(index))
    }
    
    def toMap[A <% Ordering[A]](col: IndexedSeq[A]): Map[A, Int] = {
      col.distinct.sortWith((a, b) => a.compare(a, b) <= 0).zipWithIndex.toMap

    }

    val seqMap = for (i <- 0 to dataSet.head.size - 1) yield {
      val c1 = col(i)
      toMap(c1)

    }
    println(seqMap)
    val d2 = dataSet.map { row => row.zipWithIndex.map(r => seqMap(r._2)(r._1)) }
    d2.foreach(println)
    val d3 = d2.map(r => (r.last.toDouble, r.init.map( _.toDouble)) )
    d3
  }
}