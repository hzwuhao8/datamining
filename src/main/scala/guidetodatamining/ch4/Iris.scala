package guidetodatamining.ch4

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.Pipeline

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object Iris extends util.Log {

  val path = "data/"
  val trainingfile = path + "irisTrainingSet.data.txt"
  val testfile = path + "irisTestSet.data.txt"

  val master = "local[3]"

  val appName = "iris"

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def read(filename: String) = {
      sc.textFile(filename).map { _.split("\t") }.map { arr =>
        Data(arr(0).trim.toDouble, arr(1).trim.toDouble, arr(2).trim.toDouble, arr(3).trim.toDouble,arr(4).trim)
      }.toDF()
    }
    val training = read(trainingfile)
    val test = read(testfile)

    def accuracyCount(pipeline: Pipeline) = {
      // Train model.  This also runs the indexers.
      val model = pipeline.fit(training)

      // Make predictions.
      val predictions = model.transform(test)

      // Select example rows to display.
      predictions.select("predictedLabel", "label", "features").show(5)
      val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision")

      val accuracy = evaluator.evaluate(predictions)
      accuracy
    }

    /**
     *  提取属性 和进行 转换
     *
     */
    val df = training.unionAll(test)

    val labelIndex = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
    val converter = new IndexToString().setInputCol("indexedLabel").setOutputCol("predictedLabel")

    val assembler = new VectorAssembler().setInputCols(Array("w1", "h1","w2","h2")).setOutputCol("features")

    {
      val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("features")

      val pipeline = new Pipeline().setStages(Array(labelIndex, assembler, dt, converter))
      val accuracy = accuracyCount(pipeline)
      println("Test Error = " + (1.0 - accuracy))
    }
    //随机森林

    {
      val rf = new RandomForestClassifier().setLabelCol("indexedLabel").setFeaturesCol("features").setNumTrees(3)

      val pipeline = new Pipeline().setStages(Array(labelIndex, assembler, rf, converter))

      val accuracy = accuracyCount(pipeline)
      println("随机森林 Test Error = " + (1.0 - accuracy))

    }

    //数据归一化处理
    {
      // Normalize each Vector using $L^1$ norm.
      import org.apache.spark.ml.feature.Normalizer
      val normalizer = new Normalizer()
        .setInputCol("features")
        .setOutputCol("normFeatures")
        .setP(2.0)

      {
        val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("normFeatures")

        val pipeline = new Pipeline().setStages(Array(labelIndex, assembler, normalizer, dt, converter))
        val accuracy = accuracyCount(pipeline)
        println("normFeatures ,2  Test Error = " + (1.0 - accuracy))
      }

      {
        import org.apache.spark.ml.feature.StandardScaler
        val scaler = new StandardScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")
          .setWithStd(true)
          .setWithMean(true)

        val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("scaledFeatures")

        val pipeline = new Pipeline().setStages(Array(labelIndex, assembler, scaler, dt, converter))
        val accuracy = accuracyCount(pipeline)
        println("scaledFeatures Test Error = " + (1.0 - accuracy))

      }

      {
        import org.apache.spark.ml.feature.MinMaxScaler

        val scaler = new MinMaxScaler()
          .setInputCol("features")
          .setOutputCol("scaledFeatures")

        val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("scaledFeatures")

        val pipeline = new Pipeline().setStages(Array(labelIndex, assembler, scaler, dt, converter))
        val accuracy = accuracyCount(pipeline)
        println("MinMaxScaler Test Error = " + (1.0 - accuracy))

      }

    }

  }

  case class Data(h1: Double, w1: Double, h2: Double, w2: Double, label: String)

}