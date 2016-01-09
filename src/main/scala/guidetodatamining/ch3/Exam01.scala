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

  def main(args: Array[String]) {
    val r = new CosRecommend(users3)
    r.sdev.foreach(println)

    val user = "David"
    val movie = "Kacey Musgraves"
    val res = r.pui(users3(user), movie)
    log.info(s"${user} -> ${movie} = ${res}")
    for ((movie, v) <- users3(user)) {
      log.info(s"已知是 ${v}； ${user} -> ${movie} = ${r.pui(users3(user), movie)}")
    }
    r.recommondPar(user).foreach(println)
  }

}

/**
 * 根据余弦相似度 推荐
 */

class CosRecommend(data: UserMap) extends util.Log {

  lazy val sdev = devs()
  val averages = data.par.map {
    case (k, v) =>
      k -> v.values.sum / v.size
  }

  def zipself[A](x: List[A]): List[(A, A)] = {
    x match {
      case Nil       => Nil
      case x :: Nil  => Nil
      case h :: tail => tail.map(x => (h, x)) ::: zipself(tail)
    }

  }

  def devs(): Map[String, Map[String, Double]] = {
    val bandSeq = data.flatMap { case (k, v) => v.keySet }.toSet.toList
    log.debug(s"bandSeq=${bandSeq.take(10)}")
    val pairList = zipself(bandSeq)
    log.debug(s"pairList.size=${pairList.size}")

    // 相似度矩阵
    val seq = pairList.par.flatMap {
      case (b1, b2) =>
        val c = computeSimilarity(b1, b2)
        //println(b1,b2,c)
        Seq(b1 -> (b2, c),
          b2 -> (b1, c))
    }
    val xs = new Array[(String, (String, Double))](pairList.size * 2)
    seq.copyToArray(xs)

    log.debug(s"devs ok xs.size=${xs.size},${xs.take(10).toList}")

    bandSeq.map { k =>
      val seq: Array[(String, Double)] = xs.filter { case (k1, v1) => k == k1 }.map { _._2 }
      k -> (seq ++ Array(k -> 1.0)).toMap

    }.toMap

  }

  def computeSimilarity(band1: String, band2: String): Double = {

    val z = (0.0, 0.0, 0.0)
    val (num, dem1, dem2) = data.flatMap {
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
    if (res.isNaN()) {
      //println(s"num=${num},dem1=${dem1},dem2=${dem2}")
      //throw new RuntimeException("NaN")
      0.0
    } else {
      res
    }
  }

  def nr(d: Double, min: Double, max: Double): Double = {
    if (min == max) 1 else (2 * (d - min) - (max - min)) / (max - min)
  }

  def rn(d: Double, min: Double, max: Double): Double = {
    if (min == max) 1 else 0.5 * (d + 1) * (max - min) + min
  }
  /**
   * u 对 i 的 可能的评价
   */
  def pui(u: Map[String, Double], i: String): Double = {
    u.get(i) match {
      case Some(v) => v
      case None =>

        val seq1 = u.map(_._2)
        val (min, max) = (seq1.min, seq1.max)
        // 数据归一
        val run = u.map { case (k, v) => k -> nr(v, min, max) }
        val sin = sdev(i).flatMap {
          case (k2, d) =>
            if (k2 == i) {
              None
            } else {
              Some(k2 -> d)
            }
        }.toMap

        val d1 = run.map { case (k, v) => sin.getOrElse(k, 1.0) * v }.sum
        val d2 = sin.map(kv => Math.abs(kv._2)).sum

        val d = d1 / d2
        if (d.isNaN()) {
          println(u)
          println(run)
          println(sin)
          println(d1, d2)
          throw new RuntimeException("NaN")
        }
        rn(d, min, max)
    }
  }

  //并行版本
  def recommondPar(u: String, n: Int = 20): Seq[(String, Double)] = {
    data.get(u) match {
      case None => Seq()
      case Some(udata) =>
        //取得 全部的 movies
        //计算 还有 评价的 movie
        // 按评价从高到低排列
        val movies = sdev.keySet
        val mayBe = (movies -- udata.keys)
        log.debug(s"mayBe.size=${mayBe.size}")
        val res = new Array[(String, Double)](mayBe.size)
        mayBe.par.map(m => (m, pui(udata, m))).copyToArray(res)
        res.sortBy(_._2).reverse.take(n)
    }
  }

}