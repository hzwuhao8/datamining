package guidetodatamining.ch4
 
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler

 

object Iris extends  Base with util.Log {

  val appName ="Iris"
  val path = "data/"
  val trainingfile = path + "irisTrainingSet.data.txt"
  val testfile = path + "irisTestSet.data.txt"

   def read(filename: String,sc: SparkContext,  sqlContext: SQLContext): DataFrame = {
     import sqlContext.implicits._
      sc.textFile(filename).map { _.split("\t") }.map { arr =>
        Data(arr(0).trim.toDouble, arr(1).trim.toDouble, arr(2).trim.toDouble, arr(3).trim.toDouble,arr(4).trim)
      }.toDF()
    }
  
  val assembler  = new VectorAssembler().setInputCols(Array("w1", "h1", "w2", "h2")).setOutputCol("features")

  case class Data(h1: Double, w1: Double, h2: Double, w2: Double, label: String)

}