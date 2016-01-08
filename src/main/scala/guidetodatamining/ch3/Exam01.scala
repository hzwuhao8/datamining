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
    val r =new CosRecommend(users3)
    r.sdev.foreach(println)

    val user = "David"
    val movie = "Kacey Musgraves"
    val res = r.pui(users3(user), movie)
    log.info(s"${user} -> ${movie} = ${res}")
    for ((movie, v) <- users3(user)) {
      log.info(s"已知是 ${v}； ${user} -> ${movie} = ${r.pui(users3(user), movie)}")
    }
  }

}

/**
 * 根据余弦相似度 推荐
 */

class CosRecommend(data: UserMap) extends util.Log {
  lazy val sdev = devs()
  
  def zipself[A](x: List[A]):List[(A,A)] ={
    x match{
      case Nil => Nil
      case x::Nil => Nil
      case h::tail => tail.map( x=> (h,x)) ::: zipself(tail)
    }
    
  }
  
  
  def devs(): Map[(String, String), Double] = {
    val bandSeq = data.flatMap { case (k, v) => v.keySet }.toSet.toList
    log.debug(s"bandSeq=${bandSeq.take(10)}")
    val pairList = zipself(bandSeq)
    
    
    // 相似度矩阵
    val seq = pairList.par.map { case (b1, b2) => (b1, b2) -> computeSimilarity(b1, b2) }
    val xs = new Array[((String,String), Double)](pairList.size )
    seq.copyToArray(xs)
    val m1 = xs.toMap
    val s1 = pairList.map{ case(b1,b2) =>  (b2,b1) -> m1(b1,b2)}
    val s2 = bandSeq.map{  b=> (b,b)->1.0}
    m1 ++ s1 ++ s2
  }
  def computeSimilarity(band1: String, band2: String): Double = {
    val averages = data.par.map {
      case (k, v) =>
        k -> v.values.sum / v.size
    }
    
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
  def pui(u: Map[String, Double], i: String): Double = {
    u.get(i) match {
      case Some(v) => v
      case None =>

        val seq1 = u.map(_._2)
        val (min, max) = (seq1.min, seq1.max)
        // 数据归一
        val run = u.map { case (k, v) => k -> nr(v, min, max) }
        val sin = sdev.flatMap {
          case Tuple2((k1, k2), d) =>
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
  }

}