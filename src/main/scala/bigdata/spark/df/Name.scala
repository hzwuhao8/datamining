package bigdata.spark.df

import org.ansj.splitWord.analysis._
import org.ansj.domain.Term
import scala.collection.convert.WrapAsScala._
import scala.collection.convert.WrapAsJava._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.commons.io.IOUtils

/**
 * 还是需要从 分词开始
 */

object Name extends App {
  val str = "洁面仪配合洁面深层清洁毛孔 清洁鼻孔面膜碎觉使劲挤才能出一点点皱纹 脸颊毛孔修复的看不见啦 草莓鼻历史遗留问题没辙 脸和脖子差不多颜色的皮肤才是健康的 长期使用安全健康的比同龄人显小五到十岁 28岁的妹子看看你们的鱼尾纹"

  def toNlp(str: String) = {
    val tt = ToAnalysis.parse(str)
    asScalaBuffer(tt).toSeq.map(t => (t.getName(), t.getNatureStr()))
  }

  val strL = List(str, "山野风情特色饭庄")
  //strL.foreach( s => println( p(s)) )

  case class Names(_id: Long, name: String, t: Option[Int], `type`: Option[String])

  case class NamesNlp(_id: Long, name: String, t: Option[Int], `type`: Option[String], nlp: Seq[String])

  val js01 = """{ "_id" : 1, "name" : "宝珍大馇粥", "t" : 5 }
{ "_id" : 2, "name" : "王爷府", "t" : 4 }
{ "_id" : 8, "name" : "津津味", "t" : 4 }
{ "_id" : 10, "name" : "山野风情特色饭庄", "t" : 4 }
{ "_id" : 67, "name" : "品味苑汉餐", "t" : 4 }
{ "_id" : 133, "name" : "回民火锅店", "t" : 4 }
{ "_id" : 140, "name" : "锦福餐厅", "t" : 4 }
{ "_id" : 189, "name" : "君华饺子园", "t" : 4 }
{ "_id" : 196, "name" : "美食居木火灶台鱼", "t" : 4 }
{ "_id" : 202, "name" : "十二月美食屋", "t" : 4 }
{ "_id" : 267, "name" : "贝尔肥牛", "t" : 4 }"""

  implicit val formats = DefaultFormats
  val filename = "data/names.json"
  val f = new java.io.File("/tmp/a_to.json")
  val out = new java.io.FileOutputStream(f)
  val seqS = scala.io.Source.fromFile(filename)
  val list2 = seqS.getLines().map { s =>
    val js = parse(s)

    js match {
      case JObject(x) =>
        val name = (js \ "name").extract[String]

        val t = (JField("nlp", JArray(toNlp(name).map(x => JString(x._1)).toList)) :: x.reverse).reverse
        val str = compact(render(JObject(t)))
        IOUtils.write(str  +"\n" , out)
        str
      case _ => throw new RuntimeException("err")
    }

  }
 
 list2.foreach(println)

  out.flush()

  out.close()

  val jsV = js01.split("\n").map(parse(_)).toList

  val nameSeq = for {
    JObject(x) <- jsV
    JField("name", JString(name)) <- x
  } yield {
    val t = (JField("nlp", JArray(toNlp(name).map(x => JString(x._1)).toList)) :: x.reverse).reverse
    compact(render(JObject(t)))
  }
  //nameSeq.foreach(println)

}