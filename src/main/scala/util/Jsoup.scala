package util

/**
 * fetch data
 */
import java.io.File
import scala.io.Source
import org.jsoup.Jsoup
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.io.FileUtils
object JsoupFetch extends App with Log {

  fetchA()
  
  def fetchA() {
    val url = "http://www.51zzl.com/jiaotong/feijichang.asp"
    val html = cacheFetch(url)
    val doc = Jsoup.parse(html, "GBK")
    val eList = doc.select("div a  ")
    val strList =  eList.html().split("\n").toList.filter(_.endsWith("机场"))
    strList.foreach(log.debug(_))
    // 如何 和 库里的 数据 进行结合？
    // 直接 Name  匹配吗 ？
  }

  def cacheFetch(url: String): String = {
    val filename = DigestUtils.sha256Hex(url).take(5)
    val file = new File(s"/tmp/${filename}.cache")
    val res = if (file.exists()) {
      Source.fromFile(file).getLines().mkString("\n")
    } else {
      val html = Source.fromURL(url,"GBK").getLines().mkString("\n")
      FileUtils.write(file, html)
      html
    }
   // log.debug(s"html=${res}")
    res
  }
}