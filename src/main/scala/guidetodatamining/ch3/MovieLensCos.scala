package guidetodatamining.ch3

object MovieLensCos extends util.Log with MovieData{
  
   def main(args: Array[String]) {
    val data = loadR()
    val item = loadM()
    log.info(s"data.size=${data.size}")
    val r = new CosRecommend(data)
    val sdev = r.sdev
    log.debug(s"sdev= ${sdev.take(1)}")
    List("1","25").map{ uid => 
      println(s"uid=${uid}")
      r.recommondPar(uid ).map { case (mid, v) => (mid, item.getOrElse(mid, mid), v) }.foreach(println)
      println()
      
    }
  }

   
}