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
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Exam01 extends App {

  val appName = "Dface"
  val conf = new SparkConf().setAppName(appName).setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  import sqlContext.implicits._
  val p1 = sqlContext.read.parquet("/tmp/p1txt")

 

  val assembler = new VectorAssembler().setInputCols(Array("c", "c1", "c2", "d",
    "tel1", "tel2", "password1", "password2", "menu1", "menu2",
    "t1", "t2", "utotal1", "utotal2")).setOutputCol("features")

  val labeler = new StringIndexer().setInputCol("res").setOutputCol("label")

  val pipeline = new Pipeline().setStages(Array(labeler, assembler))

  val df = pipeline.fit(p1).transform(p1)

  val df2 = df.filter("res != \"\" ")
  
  val Array(trainingData, testData) = df2.randomSplit(Array(0.7, 0.3))
  
  //df2.show();

  val featureIndexed = new VectorIndexer().
    setInputCol("features").setOutputCol("indexedFeatures").
    setMaxCategories(4).fit(df)

  val labelIndexer = new StringIndexer().setInputCol("label").
    setOutputCol("indexedLabel").fit(df)

  val rf = new RandomForestClassifier().setLabelCol("indexedLabel").
    setFeaturesCol("indexedFeatures").setNumTrees(10)

  val labelConverter = new IndexToString().setInputCol("prediction").
    setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

  val pipeline2 = new Pipeline().setStages(Array(labelIndexer, featureIndexed, rf, labelConverter))

  val model = pipeline2.fit(trainingData)
  val str = model.stages(2).asInstanceOf[RandomForestClassificationModel].toDebugString

  println(str)
  val predictions = model.transform(testData)
  predictions.select("_id", "res", "label", "prediction", "predictedLabel", "uname").limit(10).foreach(println)

  // Select (prediction, true label) and compute test error
  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setPredictionCol("prediction")
    .setMetricName("precision")
  val accuracy = evaluator.evaluate(predictions)
  println("\nTest Error = " + (1.0 - accuracy))

  
  val df3 = df.filter("res = \"\" ")

  val predictions2 = model.transform(df3)
  predictions2.select("_id", "res", "label", "prediction", "predictedLabel", "uname").show()

}