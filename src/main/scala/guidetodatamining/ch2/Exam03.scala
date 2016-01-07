package guidetodatamining.ch2

object Exam03 extends util.Log {
  val path = "data/"
  val rating = path + "Movie_Ratings.csv"

  def loadData() = {
    val lines = scala.io.Source.fromFile(rating).getLines()
    val arrList = lines.map { line =>
      val arr = (line + ",xxxxx").split(",")
      println(arr.toList)
      arr.init
    }.toList
    val head = arrList.head
    val tail = arrList.tail

    val users = head.tail.toList
    val movies = tail.map(_.head)
    val data = users.zipWithIndex.map {
      case (u, index) =>

        val data = tail.map { r =>
          log.debug(s"movie=${r(0)} , r.size=${r.size}, index=${index}")
          r(0) -> r(index + 1)
        }.filter { case (k, v) => !v.isEmpty }.map { case (k, v) => k -> v.toDouble }
        u -> data.toMap
    }
    (users, movies, data)
  }
  def main(args: Array[String]) {

    val (users,movies, data) = loadData()
    data.foreach(println)

    val r = new Recommender(data.toMap,5)
    r.recommend(users.head )
    
    val r2 = new Recommender(data.toMap , 5 , Recommender.manhattan,5)
    r2.recommend(users.head)
    
    val r3 = new Recommender(data.toMap , 5 , Recommender.eucDis,5)
    r3.recommend(users.head)
    
    
  }
}