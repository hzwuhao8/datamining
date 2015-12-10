package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.CountVectorizer
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.feature.VectorAssembler

object DfApp extends App {
  val appName = "Tree"
  val conf = new SparkConf().setAppName(appName).setMaster("local")
  val sc = new SparkContext(conf)
  val sqlSc = SQLContext.getOrCreate(sc)
  import sqlSc.implicits._

  val data = introdm.ch4.P432.dataSet2.toDF()
  data.registerTempTable("atable")

  val t1 = sqlSc.sql("select house , salary from atable where salary > 100")
  t1.map(t => "House" + t(0) + "\t" + t(1)).collect().foreach { println }

  data.show(3)
  
  val cvModel = new StringIndexer().setInputCol("house").setOutputCol("house2").fit(data)
  
  val  pass1 = cvModel.transform(data)
  pass1.show()
  val cvModel2 = new StringIndexer().setInputCol("wenxin").setOutputCol("wenxin2").fit(data)
  
  cvModel2.transform(pass1).show()
  
  val cvModel3 = new StringIndexer().setInputCol("label").setOutputCol("label2").fit(data)
  
  
  val assembler = new VectorAssembler( ).setInputCols(Array("house2","wenxin2", "salary"))
  .setOutputCol("features")
  
  val pipeline = new Pipeline().setStages(Array(cvModel, cvModel2,cvModel3,assembler))
  val m = pipeline.fit(data).transform(data)
  m.show()
  m.registerTempTable("btable")
  val lastd = sqlSc.sql("select label2,features from btable")
 
  lastd.write.parquet("/tmp/aa.txt")
 
 lastd.show()
 
  
}