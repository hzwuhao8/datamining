package guidetodatamining.ch6

import scala.io.Source
object Ihealth extends Bayes with util.Log {

  val path = "data/iHealth/i-0"
  val filename = s"${path}${1}"
  val labelindex = 4
  def main(args: Array[String]) {
    

    val data = read(filename)

    val kList = Seq("health", "moderate", "moderate", "yes")
    classify(kList)
  }

}