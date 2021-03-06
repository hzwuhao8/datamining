package guidetodatamining.ch3



/**
 * 最小的数据集的 处理
 * data/ml-100k
 */
object MovieLens extends util.Log with MovieData {
  

  def main(args: Array[String]) {
    val data = loadR()
    val item = loadM()
    val r = new SlopeOne(data)
    List("1","25").map{ uid => 
      println(s"uid=${uid}")
      r.recommondPar(uid ).map { case (mid, v) => (mid, item.getOrElse(mid, mid), v) }.foreach(println)
      println()
      
    }
  }

  

}