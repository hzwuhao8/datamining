package guidetodatamining.ch2

object Rcommender extends util.Log {
  type metricFunc = (Map[String, Double], Map[String, Double]) => Double
  type UserMap = Map[String, Map[String, Double]]

  val manhattan = minkowski(_: Map[String, Double], _: Map[String, Double], 1)
  val eucDis = minkowski(_: Map[String, Double], _: Map[String, Double], 2)

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

  def computeNearestNeighbor(username: String, users: UserMap, distance: (Map[String, Double], Map[String, Double]) => Double): Seq[(Double, String)] = {

    val u0 = users(username)
    val uSeq: Seq[(Double, String)] = users.filter { case (k, v) => k != username }.map { case (k, v) => (distance(u0, v), k) }.toList
    val rSeq = uSeq.sortBy(_._1)
    //log.debug(s"uSeq= ${uSeq}")
    log.debug(s"rSeq= ${rSeq}")
    rSeq
  }

  def recommed(username: String, users: UserMap, distance: (Map[String, Double], Map[String, Double]) => Double): Seq[(String, Double)] = {
    val nearest = computeNearestNeighbor(username, users, distance).head._2
    val neighborRatings = users(nearest)
    val userRatings = users(username)
    val maybe = neighborRatings.--(userRatings.keys)

    val res = maybe.toSeq.sortBy(_._2).reverse.toList
    log.debug(s"推荐 = username=${username} -> ${res}")
    res
  }
  val recommedManhattan = recommed(_: String, _: UserMap, manhattan)
  val recommedPearson = recommed(_: String, _: UserMap, pearson)
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
}
//推荐
//根据 最近 k 个邻居的 内容，推荐 n 个内容
class Rcommender(data: Rcommender.UserMap, k: Int = 1, metric: Rcommender.metricFunc, n: Int = 5) {

}