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
    val res = r.pui(users2(ben), "Whitney Houston")
    println(s"res=${res}")
    val res2 = r.recommond(ben)
    res2.foreach(println)
  }
}

class SlopeOne(data: UserMap) extends util.Log {
  lazy val devs: Map[(String, String), (Double, Int)] = sDev()
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

  def zipself[A](x: List[A]):List[(A,A)] ={
    x match{
      case Nil => Nil
      case x::Nil => Nil
      case h::tail => tail.map( x=> (h,x)) ::: zipself(tail)
    }
    
  }
  def sDev(): Map[(String, String), (Double, Int)] = {
    val e1 = System.currentTimeMillis()
    log.debug(s"开始计算 dev s=${e1}")
    val movies = data.flatMap { case (u, v) => v.map(_._1) }.toSet.toList
   
    val pairList = zipself(movies)
    log.debug(s"pairList.size=${pairList.size}")
    val e3 = System.currentTimeMillis()
    log.debug(s"计算 pairList  结束 e=${ e3 - e1 } ")
    
    //这个 计算 （m1,m2) -> (m2,m1) 具有 对称关系
    log.debug(s"pairList.size=${pairList.size}")
    
    val s = pairList.par.map { case (m1, m2) => (m1, m2) ->  dev(m1, m2) }

    val en = System.currentTimeMillis()
    log.debug(s"计算 dev 结束 e=${ en - e1 } ")
    val xs = new Array[(( String,String),(Double , Int) )](pairList.size )
    s.copyToArray(xs)
    log.debug(s"xs.size=${xs.size}")
    //对自身的差异
    val s1 = movies.map{ k => (k,k) ->(0.0, 0)}
    //对称
    val m1 =  xs.toMap
    val s2 = pairList.map{ case(x,y) =>
      val tmp = m1( (x,y))
      (y,x) ->  (-tmp._1 , tmp._2)
     }
    
   s1.toMap ++ s2.toMap ++ m1
    
  }
  def pui(u: Map[String, Double], j: String): Double = {
    
    val seq = u.flatMap { case (k, v) => if (k == j) None else Some(k) }
   
    val d2 = seq.map { i => devs((j,i))._2 }.sum

    val d1 = seq.map { i =>
      val h = devs((j,i))
      val devji = h._1
      val ui = u(i)
      val cji = h._2
      (devji + ui) * cji

    }.sum
    //log.debug(s"d1=${d1}, d2=${d2}")
    d1 / d2

  }

  /**
   * 推荐
   */
  def recommond(u: String): Seq[(String, Double)] = {

    data.get(u) match {
      case None => Seq()
      case Some(udata) =>
        //取得 全部的 movies
        //计算 还有 评价的 movie
        // 按评价从高到低排列
        val movies = devs.map(_._1._1).toSet
        val mayBe = (movies -- udata.keys)
        mayBe.map(m => (m, pui(udata, m))).toSeq.sortBy(_._2).reverse
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
        val movies = devs.map(_._1._1).toSet
        val mayBe = (movies -- udata.keys)
        log.debug(s"mayBe.size=${mayBe.size}")
        val res = new Array[(String, Double)]( mayBe.size)
        mayBe.par.map(m => (m, pui(udata, m))).copyToArray(res)
        res.sortBy(_._2).reverse.take(n)
    }
  }
}