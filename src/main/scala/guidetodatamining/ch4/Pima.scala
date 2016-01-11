package guidetodatamining.ch4

import org.apache.spark.SparkContext
 
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler


object Pima extends Base with util.Log {
  
   val trainingfile = path + "/pima/pima-0[1-9]"
  val testfile = path + "/pima/pima-10"

  val appName = "Pima"

  val assembler = new VectorAssembler().setInputCols(Array("c1","c2","c3","c4","c5","c6","c7","c8")).setOutputCol("features")
  
  def read(filename: String, sc: SparkContext, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    sc.textFile(filename).map { _.split("\t") }.map { arr =>
      Data(arr(0).trim.toDouble, arr(1).trim.toDouble, arr(2).trim.toDouble, arr(3).trim.toDouble,
          arr(4).trim.toDouble,arr(5).trim.toDouble,arr(6).trim.toDouble,arr(7).trim.toDouble,
          arr(8).trim
          )
    }.toDF()
  }

  case class Data(c1:Double,c2: Double,c3: Double , c4: Double,c5: Double , 
      c6: Double, c7: Double,c8: Double , label: String )
  
}