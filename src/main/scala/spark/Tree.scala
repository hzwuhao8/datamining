package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.classification.{ LogisticRegressionWithSGD, SVMModel }
import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.tree.RandomForest
object Tree extends App {
  val appName = "Tree"
  val conf = new SparkConf().setAppName(appName).setMaster("local")
  val sc = new SparkContext(conf)

  val d5 = util.DataConvert.convert(introdm.ch4.P432.dataSet)
  println(d5)
  val dd5 = d5.map(row => LabeledPoint(row._1, new DenseVector(row._2.toArray)))
  val trainingData = sc.makeRDD(dd5)
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int](0 -> 2, 1 -> 3)
  val impurity = "Gini".toLowerCase()
  val maxDepth = 5
  val maxBins = 32

  {
    val model = DecisionTree.trainClassifier(trainingData, numClasses,
      categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    println("Learned classification tree model:\n" + model.toDebugString)

    val test = d5.map { row =>
      val p = model.predict(new DenseVector(row._2.toArray))
      (row._1, p)
    }
    test.foreach(println)
  }

  {
    val model = DecisionTree.trainRegressor(trainingData,
      categoricalFeaturesInfo, "variance", maxDepth, maxBins)
    println("Learned classification tree model:\n" + model.toDebugString)
    val test = d5.map { row =>
      val p = model.predict(new DenseVector(row._2.toArray))
      (row._1, p)
    }
    test.foreach(println)
  }

  {
    val numTrees = 3
    val featureSubsetStrategy = "auto"
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    println("Learned classification tree model:\n" + model.toDebugString)
    val test = d5.map { row =>
      val p = model.predict(new DenseVector(row._2.toArray))
      (row._1, p)
    }
    test.foreach(println)

  }

  {

    val model = NaiveBayes.train(trainingData, lambda = 1.0, modelType = "multinomial")

    println("Learned classification tree model:\n" + model.toString())
    val test = d5.map { row =>
      val p = model.predict(new DenseVector(row._2.toArray))
      (row._1, p)
    }
    test.foreach(println)
  }

  {
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(trainingData)
  println("Learned classification tree model:\n" + model.toString())
    val test = d5.map { row =>
      val p = model.predict(new DenseVector(row._2.toArray))
      (row._1, p)
    }
    test.foreach(println)
  }
  sc.stop()

}