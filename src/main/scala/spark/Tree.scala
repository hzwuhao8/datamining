package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.DenseVector
object Tree extends App {
  val appName = "Tree"
  val conf = new SparkConf().setAppName(appName).setMaster("local")
  val sc = new SparkContext(conf)

  val d5 = util.DataConvert.convert(introdm.ch4.P432.dataSet)
  println(d5)
  val dd5 = d5.map(row => LabeledPoint(row._1, new DenseVector(row._2.toArray)))
  val trainingData = sc.makeRDD(dd5)
  val numClasses = 2
  val categoricalFeaturesInfo = Map[Int, Int]()
  val impurity = "Variance".toLowerCase()
  val maxDepth = 5
  val maxBins = 32

  val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
    impurity, maxDepth, maxBins)

  println("Learned classification tree model:\n" + model.toDebugString)

  sc.stop()

}