package guidetodatamining.ch2

object Exam02 extends BX{
  
  
  def main(args: Array[String]): Unit = {
    val s = System.currentTimeMillis()
    val n = 10
    val data = loadBookDBRating()
    data.take(n).foreach(println)
    val books = loadBookDBBook()
    
    val users = loadBookDBUser()
    books.take(n).foreach(println)
    users.take(n).foreach(println)
    val e1 = System.currentTimeMillis()
     println(s" load data time  ${( e1 - s )}")
    
    val r = new Recommender( data)
   
    def userRatings(id: String , n: Int=5)={
      val seq = data.getOrElse(id, Map[String ,Double]()).toSeq.sortBy(_._2).reverse
      val taks = seq.take(n)
      (seq.size ,  taks.map{ case(k,v) => books.getOrElse(k, k) -> v} )
    }
    val userid = "171118"
    val (size, m) = userRatings(userid)
    println(size)
    m.foreach(println)
    
    val res =  r.recommend(userid)
    res.foreach(println)
    res.map{ case(k,v) => books.getOrElse(k, k) -> v }.foreach(println)
    val e = System.currentTimeMillis()
     println(s" run  time  ${( e - e1)}")
  }
}

