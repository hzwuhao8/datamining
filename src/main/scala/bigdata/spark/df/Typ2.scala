package bigdata.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.feature.Word2Vec

object Typ2 {

  val master = "spark://mac06.local:7077"

  val appName = "Dface-type"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  //.setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  import sqlContext.implicits._
  val emptywhen = new Column(CaseWhen(Nil))

  val typedata = sqlContext.read.json("/tmp/dfacedata/type.json")

  val dataA = typedata.select($"_id", emptywhen.when($"t".isNotNull, 1).otherwise(0).as("t"), $"type")

  val tokenizer = new RegexTokenizer().setInputCol("type").setOutputCol("words").setPattern(";")

  val word2Vec = new Word2Vec()
    .setInputCol("words")
    .setOutputCol("features")
    .setVectorSize(10)
    .setMinCount(0)
  val labeler = new StringIndexer().setInputCol("t").setOutputCol("label")

  val d1 = labeler.fit(dataA)
  val d2 = d1.transform(dataA)
  val d3 = tokenizer.transform(d2)
  val d4 = word2Vec.fit(d3)
  val d5 = d4.transform(d3)

  

  val rf2 = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setNumTrees(7)
  val labelIndexer = new StringIndexer().setInputCol("t").
    setOutputCol("indexedLabel").fit(d5)
val d6 = labelIndexer.transform(d5)
val d7 = rf2.fit(d6) 

val t1 = d6.filter("t = 0 ") 
val modle=d7
val x1=modle.transform(t1)
val labelConverter = new IndexToString().setInputCol("prediction").
         setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

val res = labelConverter.transform(x1).filter( "predictedLabel=1")

//在这部分数据的基础， 应用  分类决策树
//期望能过得到好的结果

}