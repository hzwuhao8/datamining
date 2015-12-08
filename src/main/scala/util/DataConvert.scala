package util

import scala.math.Ordering._

/**
 * 
 * 这里 实际是有问题的 
 * 每次 属性 增加 标称值的时候， 对应关系会被破坏掉
 */
object DataConvert {

  def convert[A ](dataSet: IndexedSeq[IndexedSeq[A]]): IndexedSeq[(Double, IndexedSeq[Double])] = {
    def col(index: Int): IndexedSeq[A] = {
      dataSet.map(row => row(index))
    }
    
    def lt[A](x: A,y: A): Boolean = {
      val kv = (x,y)
      x match{
        case a: String => y match {
          case b: String => a.compareTo(b) <  0
        }
        case a: Symbol => y match{
          case b: Symbol => a.toString().compareTo(b.toString) < 0
        }
       
      }
     
     
  }
    def toMap[A](col: IndexedSeq[A]): Map[A, Int] = {
      col.distinct.sortWith( lt )
      .zipWithIndex.toMap

    }

    val seqMap = for (i <- 0 to dataSet.head.size - 1) yield {
      val c1 = col(i)
      c1.head match{
        case x: String => toMap(c1)
        case x: Symbol => toMap(c1)
        case y => Map[A,Int]()
      }
      

    }
    //println(seqMap)
    val d2 = dataSet.map { row => row.zipWithIndex.map{r => r._1 match{
      case x: Int => x.toDouble
      case x: Double => x
      case x: String => seqMap(r._2)(r._1)
      case x: Symbol => seqMap(r._2)(r._1)
      case _ => seqMap(r._2)(r._1)
    } 
      
     } }
    //d2.foreach(println)
    val d3 = d2.map(r => (r.last.toDouble, r.init.map( _.toDouble)) )
    d3
  }
}