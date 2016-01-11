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

/**
 * 用spark ml 提供的分类方法， 对数据进行分类
 *  并 测试 效果
 */
object Athletes extends util.Log {

  val path = "data/"
  val trainingfile = path + "athletesTrainingSet.txt"
  val testfile = path + "athletesTestSet.txt"

  val master = "local[3]"

  val appName = "Athletes"

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    def read(filename: String) = {
      sc.textFile(filename).map { _.split("\t") }.map { arr =>
        Data(arr(0).trim, arr(1).trim, arr(2).trim.toDouble, arr(3).trim.toDouble)
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

    val assembler = new VectorAssembler().setInputCols(Array("weight", "height")).setOutputCol("features")

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
        import org.apache.spark.ml.feature.Bucketizer
        val a1 = 0.to(300, 5).toArray
        val splits = a1.map(_.toDouble)

        val weightbucketizer = new Bucketizer()
          .setInputCol("weight")
          .setOutputCol("bucketedweight")
          .setSplits(splits)
        
        val heightbucketizer = new Bucketizer()
          .setInputCol("height")
          .setOutputCol("bucketedheight")
          .setSplits(splits)

        weightbucketizer.transform(training).show(10)
        heightbucketizer.transform(training).show(10)

        val assembler = new VectorAssembler().setInputCols(Array("bucketedweight", "bucketedheight")).setOutputCol("features")

        val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("features")

        val pipeline = new Pipeline().setStages(Array(labelIndex, weightbucketizer, heightbucketizer, assembler, dt, converter))
        val accuracy = accuracyCount(pipeline)
        println("bucketedFeatures Test Error = " + (1.0 - accuracy))

      }

    }

  }

  case class Data(name: String, label: String, weight: Double, height: Double)
}