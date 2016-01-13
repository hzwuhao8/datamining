package guidetodatamining.ch6

object Cispa extends Bayes with util.Log {
  val filename = "data/house-votes/hv-0"
  val labelindex = 0

  override def init() = {
    read((1 to 8).map(i => s"${filename}${i}"): _*)
  }
  def main(args: Array[String]) {

    val data2 = read("data/house-votes/hv-09")
    val res = data2.map {
      case label :: feature =>
        val predication = classify(feature)
        (label, predication._1)
    }
    res.foreach(println)
    val res2 = res.groupBy(x => x).map { case (k, v) => k -> v.size }
    res2.foreach(println)

  }
}