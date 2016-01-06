package advance.ch05

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.StatCounter
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._

object Exam01 extends Serializable {

  val master = "spark://mac06.local:7077"

  val appName = "Advanced Spark ch05"
  val num = 28

  val hdfs = "hdfs://n1.cdh.dface.cn:8020/user/wuhao"
  val datafile = hdfs + "/ds/kddcup.data"

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    // .setMaster(master)
    val sc = new SparkContext(conf)

    val rawData = sc.textFile(datafile)
    
    rawData.map(_.split(',').last).countByValue().toSeq.sortBy(_._2).reverse.foreach(println)
    
    
    val labelsAnddData = rawData.map{ line =>
      val buf = line.split(',').toBuffer
      buf.remove(1,3)
      val label = buf.remove(buf.length-1)
      val vector = Vectors.dense( buf.map( _.toDouble).toArray)
      (label, vector)
      
    }
    val data = labelsAnddData.values.cache()
    
    val kmeans = new KMeans()
    val model = kmeans.run(data)
    model.clusterCenters.foreach(println)
    
    
    
    val clusterLabelCount = labelsAnddData.map{ case ( label , datum) => 
      val cluster = model.predict(datum)
      (cluster ,label)
    }.countByValue
    
    clusterLabelCount.toSeq.sorted.foreach{
      case ( cluster, label) => 
        println("")
    }
    
    
    def distance(a: Vector , b: Vector) = math.sqrt( a.toArray.zip(b.toArray).map( p => p._1 - p._2).map(d => d*d).sum)
    
    def distToCenteroid(datum: Vector , model: KMeansModel) = {
      val cluster = model.predict(datum)
      val centroid = model.clusterCenters(cluster)
      distance( centroid , datum)
    }
   
   
  }
}