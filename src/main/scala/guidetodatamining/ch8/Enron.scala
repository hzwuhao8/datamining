package guidetodatamining.ch8

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeansModel

object Enron extends util.Log {
  val master = "local[3]"
  val path = "data/"
  val filename = path + "enrondata.txt"
  val appName: String = "Enron"
  case class Data(label: String, features: Vector)
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.textFile(filename).flatMap(line => {
      val arr = line.split(",")
      val label = arr(0).trim
      try {
        val features = Vectors.dense(arr.tail.map(_.toDouble))
        Some(Data(label, features))
      } catch {
        case ex: Exception => None
      }
    }).toDF()
    df.show()

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(2.0)
    val k = 8
    val kmeans = new KMeans()
      .setK(k)
      .setFeaturesCol("normFeatures")
      .setPredictionCol("prediction")

    val pipeline = new Pipeline().setStages(Array(normalizer, kmeans))
    val pipemodle = pipeline.fit(df)
    val model = pipemodle.stages.last.asInstanceOf[KMeansModel]
    println("Final Centers: ")
    model.clusterCenters.foreach(println)
    val d2 = pipemodle.transform(df).cache()
    d2.sort("prediction", "label").show()
    d2.groupBy("prediction").count().show(77, true)

    for (i <- 0 to k) {
      d2.filter(s"prediction= ${i}").select("label").sort("label").show(false)
    }
  }

}