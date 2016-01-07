package guidetodatamining.ch2

object Exam02 {
  val path = "data/BX-Dump/"
  val rating =  path + "BX-Book-Ratings.csv"
  val book = path + "BX-Books.csv"
  val users = path + "BX-Users.csv"
  
  
  def loadBookDBRating(): Recommender.UserMap = {
    val lines =  scala.io.Source.fromFile( rating).getLines() 
    val seq = lines.map{ line =>
      val fields = line.split(";")
      val user = fields(0).stripPrefix("\"").stripSuffix("\"")
      val book = fields(1).stripPrefix("\"").stripSuffix("\"")
      val rating = fields(2).trim().stripPrefix("\"").stripSuffix("\"").toDouble
      (user-> (book-> rating)  )
    }
    val z0 =Map[String, Map[String, Double]]()
    seq.foldLeft(z0){( a ,b )=> 
        a.get(b._1) match {
          case None =>  a ++ Map(b._1  -> Map(b._2._1 ->  b._2._2) ) // 还没有任何评分
          case Some(d) =>  d.get(b._2._1) match {
            case None => a ++ Map(b._1 -> d.+(b._2._1 ->  b._2._2) ) // 已经对其他书进行了评分
            case Some(dd) => a ++ Map(b._1 -> d.+(b._2._1 ->  (dd + b._2._2) ) ) //重复评分，好像是不可能的
          }
        }
    }
  }
  
  def loadBookDBBook(): Map[String,String] = {
    val lines =  scala.io.Source.fromFile( book ).getLines() 
    val seq = lines.map{ line =>
      val fields = line.split(";")
      val isbn = fields(0).stripPrefix("\"").stripSuffix("\"")
      val title = fields(1).stripPrefix("\"").stripSuffix("\"")
      val author = fields(2).trim().stripPrefix("\"").stripSuffix("\"")
      ( isbn ->  s"${title} by ${author}" )
    }
    seq.toMap
  }
  
  def loadBookDBUser(): Map[String,String] = {
    val lines =  scala.io.Source.fromFile( book ).getLines() 
    val seq = lines.map{ line =>
      val fields = line.split(";")
      val userid = fields(0).stripPrefix("\"").stripSuffix("\"")
      val location = fields(1).stripPrefix("\"").stripSuffix("\"")
      val value = if( fields.size > 3){
        val age = fields(2).trim().stripPrefix("\"").stripSuffix("\"")
        s"${location} (age:${age})"
      }else{
        location
      }
      ( userid ->  value)
    }
    seq.toMap
    
  }
  
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