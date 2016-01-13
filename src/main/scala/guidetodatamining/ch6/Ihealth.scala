package guidetodatamining.ch6

import scala.io.Source
object Ihealth extends util.Log {

  val path = "data/iHealth/i-0"

  def read(filename: String): List[List[String]] = {
    val lines = Source.fromFile(filename).getLines().toList
    val data = lines.map(_.split("\t").toList)
    data.foreach(println)
    data
  }
  def getData1(data: List[List[String]]) = {
    val columns = data.head.size
    val data1 = for (i <- 0 to columns - 1) yield { data.groupBy(x => x(i)).map { case (k, v) => k -> v.size } }
    data1.foreach(println)
    data1
  }
  def getData2(data: List[List[String]], labelindex: Int) = {
    val columns = data.head.size
    val data2 = data.groupBy { x => x(labelindex) }.map {
      case (k, v) =>
        val total = v.size
        val x = for (i <- 0 to columns - 1; if i != labelindex) yield {
          val g = v.groupBy(x => x(i))
          g.map {
            case (k, v) =>
              if (v.size == 0) {
                k -> (1.0 / (total + g.size))
              } else {
                k -> (v.size.toDouble / total)
              }
          }
        }
        k -> x
    }
    data2.foreach(println)
    data2
  }

  def main(args: Array[String]) {
    val i = 1
    val filename = s"${path}${i}"
    val data = read(filename)
    val labelindex = 4
    val kList = Seq("health", "moderate", "moderate", "yes")
    classify(kList, data,labelindex)
  }

  def classify(kList: Seq[String], data: List[List[String]], labelindex: Int): (String, Double) = {
    val data1 = getData1(data)
    val data2 = getData2(data, labelindex)
    val columns = data.head.size
    val data3 = data1(columns - 1)
    val data4 = data3.map {
      case (k, v) =>
        val p = data2(k).zip(kList).map { case (x, y) => x.getOrElse(y, 0.0) }
        val r = p.product * (v.toDouble / data3.map(_._2).sum)
        (k, r)
    }
    data4.foreach(println)
    val best = data4.toSeq.sortBy(_._2).reverse.head
    println(s"bset=${best}")
    best
  }
}