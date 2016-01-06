package advance.ch02

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.StatCounter
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation._


object Exam01  extends Serializable{
  
  val hdfs = "hdfs://n1.cdh.dface.cn:8020/user/wuhao"
  val userdatafile = hdfs + "/ds/user_artist_data.txt"
  
  val datafile = hdfs + "/ds/artist_data.txt"
  val aliasfile = hdfs + "/ds/artist_alias.txt"
  
  val master = "spark://mac06.local:7077"

  val appName = "Advanced Spark ch02"
  val num = 28
 
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    // .setMaster(master)
    val sc = new SparkContext(conf)
    val rawUserArtistData = sc.textFile(userdatafile, num)
    val rawArtistData = sc.textFile(  datafile, num )
    
    val artistByID = rawArtistData.flatMap{ line =>
      val (id , name ) = line.span( _ != '\t')
      if(name.isEmpty){
        None
      }else{
        try{
          Some (( id.toInt, name.trim))
        }catch{
          case e: NumberFormatException => None
        }
      }
      
    }
    
    val rawArtistAlias = sc.textFile(aliasfile, num)
    val artistAlias = rawArtistAlias.flatMap{ line => 
      val tokens = line.split('\t')
      if(tokens(0).isEmpty){
        None
      }else{
        Some( (tokens(0).toInt , tokens(1).toInt))
      }
    }.collectAsMap()
    
   val bArtistAlias = sc.broadcast( artistAlias)
   
   val trainData = rawUserArtistData.map{ line =>
     val Array(userID , artistID, count) = line.split(' ').map( _.toInt)
     val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
     Rating(userID,finalArtistID,count)
     
   }.cache()
   
   val model = ALS.trainImplicit(trainData, 10, 5,0.01 ,1.0)
   
   val str  = model.userFeatures.mapValues { x => x.mkString(", ") }.first()
   
   println(str)
   
  }
  
}