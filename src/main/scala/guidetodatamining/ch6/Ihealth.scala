package guidetodatamining.ch6

import scala.io.Source
object Ihealth extends util.Log {

  val path = "data/iHealth/i-0"
  def main(args: Array[String]) {
    val i = 1
    val lines = Source.fromFile(s"${path}${i}").getLines().toList
    val data = lines.map(_.split("\t").toList)
    data.foreach(println)
    val s1 = data.groupBy { x => x.last }.map { case (k, v) => k -> v.size }
    s1.foreach(println)
    val Seq(k1, k2) = s1.map(_._1).toSeq
    println(k1)
    println(k2)
    val data1 = for (i <- 0 to 4) yield { data.groupBy(x => x(i)).map { case (k, v) => k -> v.size } }
    data1.foreach(println)
    val data2 = data.groupBy { x => x.last }.map {
      case (k, v) =>
        val x = for (i <- 0 to 3) yield (v.groupBy(x => x(i)).map { case (k, v) => k -> v.size })
        k -> x
    }
    data2.foreach(println)
    val kList = Seq("health", "moderate", "moderate", "yes")

    val p1 = data2(k1).zip(kList).map { case (x, y) => x.getOrElse(y, 0) }
    println(p1)
    val p2 = data2(k2).zip(kList).map { case (x, y) => x.getOrElse(y, 0) }
    println(p2)
    val r1 = p1.product.toDouble / Math.pow(data1(4)(k1), p1.size) * (data1(4)(k1).toDouble / data1(4).map(_._2).sum)
    println(r1)

    val r2 = p2.product.toDouble / Math.pow(data1(4)(k2), p2.size) * (data1(4)(k2).toDouble / data1(4).map(_._2).sum)
    println(r2)
    val data3 = data1(4)
    data3.map{ case( k,v) => 
      val p = data2(k).zip(kList).map { case (x, y) => x.getOrElse(y, 0) } 
      val r = p.product.toDouble / Math.pow(data3(k), p2.size) * (v.toDouble / data3.map(_._2).sum)
      println(k,r)
    }
  }
}