package guidetodatamining.ch3

import guidetodatamining.ch2.Recommender.UserMap

object Exam01 extends util.Log {

  val users3: UserMap = Map("David" -> Map("Imagine Dragons" -> 3, "Daft Punk" -> 5,
    "Lorde" -> 4, "Fall Out Boy" -> 1),
    "Matt" -> Map("Imagine Dragons" -> 3, "Daft Punk" -> 4,
      "Lorde" -> 4, "Fall Out Boy" -> 1),
    "Ben" -> Map("Kacey Musgraves" -> 4, "Imagine Dragons" -> 3,
      "Lorde" -> 3, "Fall Out Boy" -> 1),
    "Chris" -> Map("Kacey Musgraves" -> 4, "Imagine Dragons" -> 4,
      "Daft Punk" -> 4, "Lorde" -> 3, "Fall Out Boy" -> 1),
    "Tori" -> Map("Kacey Musgraves" -> 5, "Imagine Dragons" -> 4,
      "Daft Punk" -> 5, "Fall Out Boy" -> 3))

  def computeSimilarity(band1: String, band2: String, userRatings: UserMap): Double = {
    val averages = userRatings.map {
      case (k, v) =>
        k -> v.values.sum / v.size
    }
    val z = (0.0, 0.0, 0.0)
    val (num, dem1, dem2) = userRatings.flatMap {
      case (user, ratings) =>
        if (ratings.contains(band1) && ratings.contains(band2)) {
          val avg = averages(user)
          val x1 = ratings(band1) - avg
          val x2 = ratings(band2) - avg
          Some(x1 * x2, x1 * x1, x2 * x2)
        } else {
          None
        }

    }.foldLeft(z) { (a, b) =>
      (a._1 + b._1, a._2 + b._2, a._3 + b._3)
    }
    val res = num / (Math.sqrt(dem1 * dem2))
    res
  }
  
  
  def main(args: Array[String]) {
    val bandSeq = users3.flatMap { case (k, v) => v.keySet }.toSet.toSeq
    log.debug(s"bandSeq=${bandSeq}")
    val seq = bandSeq.sliding(2).toList.map { case Seq(b1, b2) => (b1, b2, computeSimilarity(b1, b2, users3)) }
    seq.foreach(println)
  }

}