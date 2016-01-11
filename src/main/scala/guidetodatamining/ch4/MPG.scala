package guidetodatamining.ch4


import org.apache.spark.SparkContext
 
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler

object MPG extends Base with util.Log {
  
   val appName ="MPG"
  val trainingfile = path + "mpgTrainingSet.txt"
  val testfile = path + "mpgTestSet.txt"

   def read(filename: String,sc: SparkContext,  sqlContext: SQLContext): DataFrame = {
     import sqlContext.implicits._
      sc.textFile(filename).map { _.split("\t") }.map { arr =>
        Data(arr(0).trim ,arr(1).trim.toDouble, arr(2).trim.toDouble, arr(3).trim.toDouble, 
            arr(4).trim.toDouble,arr(5).trim.toDouble , arr(6).trim)
      }.toDF()
    }
  
  val assembler  = new VectorAssembler().setInputCols(Array("c1","c2","c3","c4","c5")).setOutputCol("features")

  case class Data(label:String , c1: Double, c2: Double, c3: Double, c4: Double, c5: Double ,comment: String)

  
}