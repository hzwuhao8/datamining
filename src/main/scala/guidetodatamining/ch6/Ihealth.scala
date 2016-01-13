package guidetodatamining.ch6

import scala.io.Source
object Ihealth extends Bayes with util.Log {

  val path = "data/iHealth/i-0"

   def main(args: Array[String]) {
    val i = 1
    val filename = s"${path}${i}"
    val data = read(filename)
    val labelindex = 4
    val kList = Seq("health", "moderate", "moderate", "yes")
    classify(kList, data,labelindex)
  }
    
}