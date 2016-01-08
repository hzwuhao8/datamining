package guidetodatamining.ch3

import guidetodatamining.ch2.Recommender.UserMap

object SlopeOne extends util.Log {

  val users2: UserMap = Map("Amy" -> Map("Taylor Swift" -> 4, "PSY" -> 3, "Whitney Houston" -> 4),
    "Ben" -> Map("Taylor Swift" -> 5, "PSY" -> 2),
    "Clara" -> Map("PSY" -> 3.5, "Whitney Houston" -> 4),
    "Daisy" -> Map("Taylor Swift" -> 5, "Whitney Houston" -> 3))

  
    
  def main(args: Array[String]) {
    val r = new SlopeOne(users2)
    val s = r.devs
    s.foreach(println)
    val ben = "Ben"
    val res = r.pui(users2(ben ), "Whitney Houston" )
    println(s"res=${res}")
    val res2 = r.recommond(ben)
    res2.foreach(println)
  }
}

class  SlopeOne(data: UserMap) extends util.Log {
  lazy val  devs : Seq[(String, String, (Double, Int))] = sDev()
  def dev(i: String, j: String): (Double, Int) = {
    if (i == j) {
      (0.0, 0)
    } else {
      val seq = data.flatMap {
        case (u, v) =>
          if (v.contains(i) && v.contains(j)) {
            val ui = v(i)
            val uj = v(j)
            Some(ui - uj)
          } else {
            None
          }
      }
      if (seq.size == 0) {
        (0.0, 0)
      } else {
        ((seq.sum) / seq.size, seq.size)
      }
    }

  }

  def sDev(): Seq[(String, String, (Double, Int))] = {
    val movies = data.flatMap { case (u, v) => v.map(_._1) }.toSet.toSeq
    val pairList = movies.flatMap(x => movies.map(y => (x, y)))
    val s = pairList.map { case (m1, m2) => (m1, m2, dev(m1, m2)) }
    s
  }
  def pui(u: Map[String, Double], j: String): Double = {
    val seq = u.flatMap { case (k, v) => if (k == j) None else Some(k) }
    val d2 = seq.flatMap { i => devs.filter(p => p._1 == j && p._2 == i).map(_._3._2) }.sum

    val d1 = seq.map { i =>
      val h = devs.filter(p => p._1 == j && p._2 == i).head
      val devji = h._3._1
      val ui = u(i)
      val cji = h._3._2
      (devji + ui) * cji

    }.sum
    d1 / d2

  }
 
  /**
   * 推荐
   */
  def recommond(u:String   ): Seq[(String , Double)] = {
     
      data.get(u) match{
      case None => Seq()
      case Some(udata) => 
        //取得 全部的 movies
        //计算 还有 评价的 movie
        // 按评价从高到低排列
        val movies = devs.map(_._1).toSet
        val mayBe = ( movies -- udata.keys)
        mayBe.map( m => (m , pui( udata , m  ))).toSeq.sortBy( _._2).reverse 
    }
  }
}