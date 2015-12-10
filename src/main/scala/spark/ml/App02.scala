package spark.ml

/**
 *  use ml pipeline
 */

import org.apache.spark.ml
import ml.Pipeline
import ml.regression.RandomForestRegressionModel
import ml.regression.RandomForestRegressor
import ml.evaluation.RegressionEvaluator
import ml.feature.VectorIndexer
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString

object App02 extends App {

  
  val appName = "Tree"
  val conf = new SparkConf().setAppName(appName).setMaster("local")
  val sc = new SparkContext(conf)
  val sqlSc = SQLContext.getOrCreate(sc)
  val dd = sqlSc.read.parquet("/tmp/aa.txt")
  val df =  dd.toDF("label", "features")

  val featureIndexed = new VectorIndexer().
    setInputCol("features").setOutputCol("indexedFeatures").
    setMaxCategories(4).fit(df)

  val labelIndexer = new StringIndexer().setInputCol("label").
    setOutputCol("indexedLabel").fit(df)

  val rf = new RandomForestClassifier().setLabelCol("indexedLabel").
    setFeaturesCol("indexedFeatures").setNumTrees(3)

  val labelConverter = new IndexToString().setInputCol("prediction").
    setOutputCol("predictedLabel").setLabels(labelIndexer.labels)

  val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexed, rf, labelConverter))

  val model = pipeline.fit(df)
 
  val predictions = model.transform(df)
  // Select example rows to display.
  predictions.select("predictedLabel", "label", "features").show(5)

}