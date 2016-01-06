package guidetodatamining.ch2

object Exam01 extends util.Log {
  type UserMap = Map[String, Map[String, Double]]

  val users: UserMap = Map("Angelica" -> Map("Blues Traveler" -> 3.5, "Broken Bells" -> 2.0, "Norah Jones" -> 4.5, "Phoenix" -> 5.0, "Slightly Stoopid" -> 1.5, "The Strokes" -> 2.5, "Vampire Weekend" -> 2.0),
    "Bill" -> Map("Blues Traveler" -> 2.0, "Broken Bells" -> 3.5, "Deadmau5" -> 4.0, "Phoenix" -> 2.0, "Slightly Stoopid" -> 3.5, "Vampire Weekend" -> 3.0),
    "Chan" -> Map("Blues Traveler" -> 5.0, "Broken Bells" -> 1.0, "Deadmau5" -> 1.0, "Norah Jones" -> 3.0, "Phoenix" -> 5, "Slightly Stoopid" -> 1.0),
    "Dan" -> Map("Blues Traveler" -> 3.0, "Broken Bells" -> 4.0, "Deadmau5" -> 4.5, "Phoenix" -> 3.0, "Slightly Stoopid" -> 4.5, "The Strokes" -> 4.0, "Vampire Weekend" -> 2.0),
    "Hailey" -> Map("Broken Bells" -> 4.0, "Deadmau5" -> 1.0, "Norah Jones" -> 4.0, "The Strokes" -> 4.0, "Vampire Weekend" -> 1.0),
    "Jordyn" -> Map("Broken Bells" -> 4.5, "Deadmau5" -> 4.0, "Norah Jones" -> 5.0, "Phoenix" -> 5.0, "Slightly Stoopid" -> 4.5, "The Strokes" -> 4.0, "Vampire Weekend" -> 4.0),
    "Sam" -> Map("Blues Traveler" -> 5.0, "Broken Bells" -> 2.0, "Norah Jones" -> 3.0, "Phoenix" -> 5.0, "Slightly Stoopid" -> 4.0, "The Strokes" -> 5.0),
    "Veronica" -> Map("Blues Traveler" -> 3.0, "Norah Jones" -> 5.0, "Phoenix" -> 4.0, "Slightly Stoopid" -> 2.5, "The Strokes" -> 3.0))

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
  def main(args: Array[String]): Unit = {
    val uh = "Hailey"
    val ua = "Angelica"
    val ub = "Bill"
    val uj = "Jordyn"
    val List(u1, u2) = List(uh, "Veronica")
    log.info("距离 = {}", manhattan(users(u1), users(u2)))

    log.info("距离 = {}", manhattan(users(uh), users(uj)))
    computeNearestNeighbor(uh, users, manhattan)
    recommedManhattan(uh, users)

    recommedManhattan("Chan", users)

    recommedManhattan("Sam", users)
    recommedManhattan("Angelica", users)

    log.info(s"pearson ${ua},${ub} = {} ", pearson(users(ua), users(ub)))
    log.info(s"pearson ${ua},${uh} = {} ", pearson(users(ua), users(uh)))
    log.info(s"pearson ${ua},${uj} = {} ", pearson(users(ua), users(uj)))
    recommedManhattan(uh, users)
    recommedPearson(uh, users)
  }
}