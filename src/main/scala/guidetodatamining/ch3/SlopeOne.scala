package guidetodatamining.ch3

import guidetodatamining.ch2.Recommender.UserMap

object SlopeOne  extends util.Log{
  
  
  val users2: UserMap = Map("Amy"-> Map("Taylor Swift"-> 4, "PSY"-> 3, "Whitney Houston"-> 4),
          "Ben"-> Map("Taylor Swift"-> 5, "PSY"-> 2),
          "Clara"-> Map("PSY"-> 3.5, "Whitney Houston"-> 4),
          "Daisy"-> Map("Taylor Swift"-> 5, "Whitney Houston"-> 3))

  
  def dev(i: String , j: String, data: UserMap) : (Double,Int) = {
    if( i==j){
      (0.0,0)
    }else{
      val seq = data.flatMap{ case(u , v) =>
        if( v.contains(i) && v.contains(j)){
          val ui = v(i)
          val uj = v(j)
          Some(ui -uj)
        }else{
          None
        }
      }
      if(seq.size ==0){
          (0.0,0)
      }else{
      ( (seq.sum) /seq.size , seq.size)
      }
    }
     
  }
  
  def pui(u: Map[String,Double] , j: String  , s: Seq[(String,String , (Double , Int))]   ): Double ={
    val seq = u.flatMap{ case( k,v) => if( k==j ) None else Some( k ) }
    val d2 = seq.flatMap{ i  => s.filter( p=> p._1 == j && p._2 == i).map(   _._3._2) }.sum
    
    val d1 = seq.map{ i => 
      val h =  s.filter( p=> p._1 == j && p._2 == i).head
      val devji = h._3._1
      val ui = u(i)
      val cji = h._3._2
      (devji + ui ) * cji
      
    }.sum
    d1 /d2     
   
  }
  def main(args: Array[String]) {
    val movies = users2.flatMap{ case( u ,v) => v.map(_._1)}.toSet.toSeq
    val pairList = movies.flatMap( x => movies.map(y=> (x,y)) )
    val s =   pairList.map{case (m1,m2) => (m1,m2, dev(m1,m2,users2))}   
    s.foreach(println)
    
    val res =  pui( users2("Ben") , "Whitney Houston" , s)
    println(s"res=${res}")
  }
}