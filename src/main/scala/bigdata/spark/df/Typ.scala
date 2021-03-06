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
import org.apache.spark.ml.feature.OneHotEncoder

/**
 *
 */
object Typ extends App {
  val master="spark://mac06.local:7077"

  val appName = "Dface-type"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  //.setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  import sqlContext.implicits._
val emptywhen = new Column( CaseWhen(Nil))
  
  val typedata = sqlContext.read.json("/tmp/dfacedata/type.json")
  
  val data = typedata.select($"_id", emptywhen.when($"t".isNotNull, $"t").otherwise(-1).as("t"), $"type" )
  //.filter("t is not null ")
  //.sample(false , 0.01)
  
  val tokenizer = new RegexTokenizer().setInputCol("type").setOutputCol("words").setPattern(";")

  val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

val word2Vec = new Word2Vec()
  .setInputCol("words")
  .setOutputCol("features")
  .setVectorSize(10)
  .setMinCount(0)

val type2Features = new StringIndexer().setInputCol("type").setOutputCol("features")


  val labeler = new StringIndexer().setInputCol("t").setOutputCol("label")

  //val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf))

  val pipeline = new Pipeline().setStages(Array(tokenizer,word2Vec )) 

  val df = pipeline.fit(data).transform(data)

  val df2 = df.filter("t  >= 0 ")

  val Array(trainingData, testData) = df2.randomSplit(Array(0.75, 0.25))

  val featureIndexed = new VectorIndexer().
    setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(1000) 
val oneHotencoder = new OneHotEncoder().setInputCol("features").
  setOutputCol("indexedFeatures")

    


  val labelIndexer = new StringIndexer().setInputCol("t").
    setOutputCol("indexedLabel").fit(df)

  //val rf = new RandomForestClassifier().setLabelCol("indexedLabel").
  //  setFeaturesCol("indexedFeatures").setNumTrees(3)
  val rf2 = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setNumTrees(3)

  val rf3 = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setNumTrees(3)
  

  val labelConverter = new IndexToString().setInputCol("prediction").
    setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

  //val pipeline2 = new Pipeline().setStages(Array(labelIndexer, featureIndexed, rf, labelConverter))
  val pipeline2 = new Pipeline().setStages(Array(labelIndexer,  rf2, labelConverter))
  val model = pipeline2.fit(trainingData)
  val str = model.stages(1).asInstanceOf[RandomForestClassificationModel].toDebugString
  
   
  
  println(str)
  val predictions = model.transform(testData)
  predictions.select("_id", "type", "prediction", "indexedLabel", "predictedLabel", "t").limit(10).foreach(println)

  // Select (prediction, true label) and compute test error
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setPredictionCol("prediction")
    .setMetricName("precision")
  val accuracy = evaluator.evaluate(predictions)
  println("\nTest Error = " + (1.0 - accuracy))

  val df3 = df.filter("t = -1  ")
  val result = model.transform(df3)
  result.write.json("/tmp/c1.txt")

  result.select("_id", "type", "prediction", "indexedLabel", "predictedLabel", "t").limit(25).foreach(println)

}
