package guidetodatamining.ch4

 

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext

 

/**
 * 用spark ml 提供的分类方法， 对数据进行分类
 *  并 测试 效果
 */
object Athletes extends Base with util.Log {

  
  val trainingfile = path + "athletesTrainingSet.txt"
  val testfile = path + "athletesTestSet.txt"

  val appName = "Athletes"

  val assembler = new VectorAssembler().setInputCols(Array("w", "h")).setOutputCol("features")
  
  def read(filename: String, sc: SparkContext, sqlContext: SQLContext) = {
    import sqlContext.implicits._
    sc.textFile(filename).map { _.split("\t") }.map { arr =>
      Data(arr(0).trim, arr(1).trim, arr(2).trim.toDouble, arr(3).trim.toDouble)
    }.toDF()
  }

  case class Data(name: String, label: String, w: Double, h: Double)
}