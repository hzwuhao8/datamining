package guidetodatamining.ch3

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object MovieLensSpark extends util.Log {
  val path = "data/ml-100k/"
  val ratingfile = path + "u.data"
  val userfile = path + "u.user"
  val moviefile = path + "u.item"

  val master = "local[3]"

  val appName = "MovieLensSpark"

  def main(args: Array[String]) {
    val ratings = loadR()
    val movies = loadM()
    
    val conf = new SparkConf().setAppName(appName).setMaster(master)

    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(ratings)
    val rank = 5
    val numIterations = 10
    val model = ALS.train(rdd, rank, numIterations, 0.01)
    List(1,25).map{uid =>
      println(s"uid=${uid}")
      model.recommendProducts(uid, 10).map{ r=>
        (r,   movies.getOrElse(r.product.toString,r.product))
      }.foreach(println)
    }
  }

  def loadR(): Seq[Rating] = {
    val lines = scala.io.Source.fromFile(ratingfile).getLines()
    val seq = lines.map { line =>
      val fields = line.split("\t")
      val Array(uid, itemid, rating, _) = fields
      new Rating(uid.toInt, itemid.toInt, rating.toDouble)

    }.toSeq
    seq
  }

  def loadM(): Map[String, String] = {
    val lines = org.apache.commons.io.FileUtils.readLines(new java.io.File(moviefile))

    val seq = scala.collection.convert.WrapAsScala.asScalaBuffer(lines).flatMap { line =>
      val fields = line.split("\\|")
      val res = if (fields.size >= 2) {
        val mid = fields(0)
        val name = fields(1)
        Some((mid -> name))
      } else {
        None
      }
      res
    }
    seq.toMap
  }

}