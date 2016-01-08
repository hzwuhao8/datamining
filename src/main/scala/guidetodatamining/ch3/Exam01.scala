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

  def nr(d: Double, min: Double, max: Double): Double = {

    (2 * (d - min) - (max - min)) / (max - min)
  }

  def rn(d: Double, min: Double, max: Double): Double = {

    0.5 * (d + 1) * (max - min) + min
  }
  /**
   * u 对 i 的 可能的评价
   */
  def pui(u: Map[String, Double], i: String, s: Seq[(String, String, Double)]): Double = {
    val seq1 = u.map(_._2)
    val (min, max) = (seq1.min, seq1.max)
    // 数据归一
    val run = u.map { case (k, v) => k -> nr(v, min, max) }
    val sin = s.flatMap {
      case Tuple3(k1, k2, d) =>
        if (k1 != i || k2 == i) {
          None
        } else {
          Some(k2 -> d)
        }
    }.toMap

    val d1 = run.map { case (k, v) => sin.getOrElse(k, 1.0) * v }.sum
    val d2 = sin.map(kv => Math.abs(kv._2)).sum

    val d = d1 / d2
    rn(d, min, max)

  }
  def main(args: Array[String]) {
    val bandSeq = users3.flatMap { case (k, v) => v.keySet }.toSet.toSeq
    log.debug(s"bandSeq=${bandSeq}")
    val pairList = bandSeq.flatMap { x =>
      bandSeq.flatMap { y =>
        if (x == y) Some(( x,y)) else Some((x, y))
      }
    }.toList
    // 相似度矩阵
    val seq = pairList.map { case (b1, b2) => (b1, b2, computeSimilarity(b1, b2, users3)) }
    seq.foreach(println)
    val user = "David"
    val movie = "Kacey Musgraves"
    val  res = pui(users3(user), movie , seq )
    log.info(s"${user} -> ${movie} = ${res}")
    for( (movie,v) <- users3(user)){
      log.info(s"已知是 ${v}； ${user} -> ${movie} = ${  pui(users3(user), movie , seq )}")
    }
  }

}