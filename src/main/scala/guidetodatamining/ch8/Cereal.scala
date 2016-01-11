package guidetodatamining.ch8

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeansModel

object Cereal extends util.Log {
  val master = "local[3]"
  val path = "data/"
  val filename = path + "cereal.csv"
  val appName: String = "cereal"

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(filename)
    df.show()

    val assembler = new VectorAssembler().setInputCols(Array("Calories", "Protein",
        "Fat (g)","Sodium (mg)","dietary fiber (g)","carbohydrates (g)","sugar","x","v")).setOutputCol("features")

    val normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(2.0)


    
    val kmeans = new KMeans()
      .setK(8)
      .setFeaturesCol("features")
      .setPredictionCol("prediction")
      
      val pipeline = new Pipeline().setStages(Array(assembler, normalizer, kmeans))
      val pipemodle = pipeline.fit(df)
    val model = pipemodle.stages(2).asInstanceOf[KMeansModel]
    println("Final Centers: ")
    model.clusterCenters.foreach(println)

    pipemodle.transform(df).sort("prediction", "Name").show(77, true)
  }
  
}