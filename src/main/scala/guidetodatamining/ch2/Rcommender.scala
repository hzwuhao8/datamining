package guidetodatamining.ch2

object Rcommender extends util.Log {
  type MetricFunc = Function2[Map[String, Double], Map[String, Double], Double]
  type UserMap = Map[String, Map[String, Double]]

  val manhattan: MetricFunc = minkowski(_: Map[String, Double], _: Map[String, Double], 1)
  val eucDis: MetricFunc = minkowski(_: Map[String, Double], _: Map[String, Double], 2)
  val pearsonMetric: MetricFunc = (pearsonReverse(_: Map[String, Double], _: Map[String, Double]))

  def minkowski(r1: Map[String, Double], r2: Map[String, Double], r: Int): Double = {
    val keys = r1.keySet.intersect(r2.keySet).toSeq
    if (keys.isEmpty) {
      0.0
    } else {
      val seq: Seq[Double] = keys.map { k =>
        val d1 = Math.abs(r1(k) - r2(k))
        Math.pow(d1, r)
      }
      val sumres = seq.sum
      Math.pow(sumres, 1 / r.toDouble)
    }
  }

  def pearsonReverse(r1: Map[String, Double], r2: Map[String, Double]): Double = {
    -pearson(r1, r2)
  }
  def pearson(r1: Map[String, Double], r2: Map[String, Double]): Double = {
    val keys = r1.keySet.intersect(r2.keySet).toSeq
    if (keys.isEmpty) {
      0.0
    } else {
      val seq = keys.map { k =>
        val (x, y) = (r1(k), r2(k))
        (x, x * x, y, y * y, x * y)
      }
      val z0 = (0.0, 0.0, 0.0, 0.0, 0.0)
      val n = keys.size
      val (sumx, sumxx, sumy, sumyy, sumxy) = seq.foldLeft(z0)((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5))
      val den = Math.sqrt((sumxx - sumx * sumx / n) * (sumyy - sumy * sumy / n))
      if (den == 0.0) {
        0.0
      } else {
        (sumxy - (sumx * sumy) / n) / den
      }
    }

  }

  def computeNearestNeighbor(username: String, users: UserMap, distance: MetricFunc): Seq[(Double, String)] = {

    val u0 = users(username)
    val uSeq: Seq[(Double, String)] = users.filter { case (k, v) => k != username }.map { case (k, v) => (distance(u0, v), k) }.toList
    val rSeq = uSeq.sortBy(_._1) // 这里有问题， 距离是 接近0 , 表示 相似， 但 pearson 系数是 接近1 表示相似！
    //log.debug(s"uSeq= ${uSeq}")
    log.debug(s"最近的邻居 = ${rSeq}")
    rSeq
  }

  def recommed(username: String, users: UserMap, distance: MetricFunc, k: Int = 1, n: Int = 5): Seq[(String, Double)] = {
    val userRatings = users(username)
    val nearestUserSeq = computeNearestNeighbor(username, users, distance).take(k)
    val totalDistance = nearestUserSeq.map(_._1).sum
    val weightSeq: Map[String, Double] = nearestUserSeq.map(x => x._2 -> x._1 / totalDistance).toMap

    val neighborRatingsSeq: Seq[Map[String, Double]] = nearestUserSeq.map { nearest =>
      val seq = users(nearest._2).--(userRatings.keys)
      val weight = if (totalDistance == 0) 1 else nearest._1 / totalDistance
      seq.map(kv => (kv._1, kv._2 * weight))
    }
    log.debug(s"neighborRatingsSeq=${neighborRatingsSeq}")
    val z0 = Map[String, Double]()
    val maybe = neighborRatingsSeq.foldLeft(z0)((a, b) => {
      val res = for ((k, v) <- b) yield {
        a.get(k) match {
          case None    => (k -> v)
          case Some(x) => (k -> (x + v))
        }
      }
      res
    })

    val res = maybe.toSeq.sortBy(_._2).reverse.toList
    log.debug(s"推荐 = username=${username} -> ${res}")
    if (res.size > n) {
      res.take(n)
    } else {
      res
    }
  }
  val recommedManhattan = recommed(_: String, _: UserMap, manhattan)
  val recommedPearson = recommed(_: String, _: UserMap, pearson)

}
//推荐
//根据 最近 k 个邻居的 内容，推荐 n 个内容
class Rcommender(data: Rcommender.UserMap, k: Int = 1, metric: Rcommender.MetricFunc= Rcommender.pearsonMetric, n: Int = 5) {
  def recommend(username: String): Seq[(String, Double)] = Rcommender.recommed(username, data, metric, k, n)
}