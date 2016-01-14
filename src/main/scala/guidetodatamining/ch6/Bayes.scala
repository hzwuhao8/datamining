package guidetodatamining.ch6

/**
 * P(h|D) = P(h^D) /P(D)
 * P(D|h) = P(D^h) /P(h)
 * P(h^D) == P(D^h)
 * 所以
 * P(h|D)*P(D)= P(h^D)=P(D|h)*P(h)
 *
 */
import scala.io.Source

trait Bayes extends util.Log {
  val filename: String 
  val labelindex: Int 
  def getData1(data: List[List[String]]) = {
   
    val data1 = for (i <- 0 to columns - 1) yield { data.groupBy(x => x(i)).map { case (k, v) => k -> v.size } }

    log.debug(s"data1=\n${data1}")
    data1
  }
  def getData2(data: List[List[String]], labelindex: Int) = {
    
    val data2 = data.groupBy { x => x(labelindex) }.map {
      case (k, v) =>
        val total = v.size
        val x = for (i <- 0 to columns - 1; if i != labelindex) yield {
          val g = v.groupBy(x => x(i))
          val m = g.size
          g.map {
            case (k, v) =>
              k -> ((v.size.toDouble + 1.0) / (total + m))
          }
        }
        k -> x
    }
    log.debug(s"data2=\n${data2}")
    data2
  }

  def init() : List[List[String]] ={
    read(filename)
  }
  
  lazy val data =  init()
  lazy val data1 = getData1(data)
  lazy val data2 = getData2(data, labelindex)
  lazy val columns = data.head.size
  lazy val data3 = data1(labelindex)

  def classify(feature: Seq[String]): (String, Double) = {

    log.debug(s"data3=\n${data3}")
    val sum = data3.map(_._2).sum
    val data4 = data3.map {
      case (k, v) =>
        val p = data2(k).zip(feature).map { case (x, y) => x.getOrElse(y, 0.0) }
        val r = p.product * (v.toDouble / sum )
        (k, r)
    }
    log.debug(s"data4=\n${data4}")
    val best = data4.toSeq.sortBy(_._2).reverse.head
    log.debug(s"bset=${best}")
    best
  }

  def read(filename: String): List[List[String]] = {
    val lines = Source.fromFile(filename).getLines().toList
    val data = lines.map(_.split("\t").toList)
    data.foreach(println)
    data
  }
  
  def read(filename: String*): List[List[String]] ={
    filename.flatMap(f => read(f)).toList
  }

}