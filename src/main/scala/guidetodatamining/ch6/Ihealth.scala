package guidetodatamining.ch6

import scala.io.Source
object Ihealth extends util.Log {

  val path = "data/iHealth/i-0"
  def main(args: Array[String]) {
    val i = 1
    val lines = Source.fromFile(s"${path}${i}").getLines().toList
    val data = lines.map(_.split("\t").toList)
    data.foreach(println)
    val columns = 5
    val data1 = for (i <- 0 to columns - 1) yield { data.groupBy(x => x(i)).map { case (k, v) => k -> v.size } }
    data1.foreach(println)

    val data2 = data.groupBy { x => x.last }.map {
      case (k, v) =>
        val total = v.size
        val x = for (i <- 0 to columns - 2) yield {
          val g = v.groupBy(x => x(i))
          g.map {
            case (k, v) =>
              
              if (v.size == 0) {
                k -> ( 1.0/(total + g.size ) )
              } else {
                k -> (v.size.toDouble / total)
              }
          }
        }
        k -> x
    }
    data2.foreach(println)
    val kList = Seq("health", "moderate", "aggressive", "yes")

    val data3 = data1(columns - 1)
    classify(kList, data2, data3)
  }

  def classify(kList: Seq[String], data2: Map[String, Seq[Map[String, Double]]], data3: Map[String, Int]): (String, Double) = {
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