package bigdata.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

import org.apache.spark.ml.feature.Word2Vec

/**
 * 标识出 哪些 type 是可以识别的
 *
 */
object Typ3 extends App {
  val master = "spark://mac06.local:7077"

  val appName = "Dface-type"
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  //.setMaster("local[4]")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  import sqlContext.implicits._
  val emptywhen = new Column(CaseWhen(Nil))

  val typedata = sqlContext.read.json("/tmp/dfacedata/type.json")
  val d1 = typedata.filter("t is   null")
  val d2 = typedata.filter("t is not null")
  val g1 = d2.groupBy("type", "t").count()
  
  val g2 = g1.groupBy("type").count()
  
  /**
   * 从 g2 中找出 只有一条 记录的数据 ， 只有一个对应关系
   */
  
  val s1 = g2.filter("count = 1 ").select("type").join(g1, "type").select("type","t","count").filter("count > 100 ")
  
  val s2 = g2.filter("count > 1  ").select("type").join(g1, "type").filter("count > 100 ")
 
  val s = s1.unionAll(s2).select($"type", $"t".as("t1") )
  
  val readydata = d1.join(s,"type").filter("t1 is not null").sort("_id")
  // 根据 t1  生成  update  语句
  
 
  
  /**
   * 这3个分类的数据 是不准的
   */
  val xt1="生活服务;生活服务场所;生活服务场所"
  val xt2= "生活服务;洗浴推拿场所;洗浴推拿场所"
  val xt3 = "生活服务;洗衣店;洗衣店"
  
  val readydata2 = readydata.filter( !( ($"type").isin(xt1,xt2,xt3))    )
  
   val cmdData2 = readydata2.map{row => 
    val id = row.getAs[Long]("_id") 
    val res = "type2t"
    val typ = row.getAs[String]("type")
    val t = row.getAs[Double]("t1")
    s"""db.shops.update({_id: $id, t:{$$exists:false}},{$$set: {t: ${t.toLong}  , reason:"${res}"}});//${typ}"""
  }
  cmdData2.take(10).foreach(println)
  
  cmdData2.saveAsTextFile("/tmp/type2t.txt")
  // 分类数据的分布这个是准备 应用的
  val sortedData = readydata2.groupBy("type","t1").count().sort( ("count")) 
  
}