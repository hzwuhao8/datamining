package bigdata.spark.df

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.CaseWhen

/**
 * 通过 spark ,将数据 转成 后续处理的 格式， 一个 js 文件， 直接用 mongo 执行
 * 原始数据  =>  { delid , reason }
 * 再设法 转成 命令
 *  
 * 
 * db.shops.update({_id: 2030511},{$set: {del:1 , reason:"2YES,2030511,33391449"}})
 * 
 * 对这个 js 文件 用 mongo 导入 运行
 * mongo shop  > /tmp/
 */
object Similar  extends App {
  
  val appName = "Dface"
  val conf = new SparkConf().setAppName(appName).setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = SQLContext.getOrCreate(sc)
  import sqlContext.implicits._
  val similar3a = sqlContext.read.json("/tmp/dfacedata/similar3a.0573.json")

  val emptywhen = new Column( CaseWhen(Nil))
  val data = similar3a.filter("res = '1YES' or res='2YES' ").select(
      emptywhen.when( $"res" === "1YES", $"s2.id").otherwise($"s1.id").as("id"), 
      $"res",$"name",$"_id",$"s1.id".as("id1"),$"s1.name".as("name1"),$"s2.id".as("id2"),$"s2.name".as("name2")
      )
 val cmdData = data.map{row => 
    val id = row.getAs[Long]("id") 
    val res = row.getAs[String]("res")
    val id1 = row.getAs[Long]("id1")
    val id2 = row.getAs[Long]("id2")
    s"""db.shops.update({_id: $id},{$$set: {del:1 , reason:"${res},${id1},${id2}"}})"""
  }
  cmdData.saveAsTextFile("/tmp/0573.txt")
  
}