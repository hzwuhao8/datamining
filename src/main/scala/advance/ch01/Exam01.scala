package advance.ch01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.util.StatCounter
import org.apache.spark.rdd.RDD

object Exam01 extends Serializable {
  val hdfs = "hdfs://n1.cdh.dface.cn:8020/user/wuhao"
  val files = hdfs + "/linkage"

  val master = "spark://mac06.local:7077"

  val appName = "Dface-type"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName(appName).setMaster(master)
    // .setMaster(master)
    val sc = new SparkContext(conf)

    val rawblocks = sc.textFile(files)

    val noheader = rawblocks.filter(!isHeader(_))
    val total = noheader.count()
    println(s"total = ${total}")

    val parsed = noheader.map(parser).cache()

    val nasRDD = parsed.map(md => md.scores.map(d => NAStatCounter(d)))

    val statsm = statsWithMissing(parsed.filter(_.matched).map(_.scores))
    val statsn = statsWithMissing(parsed.filter(!_.matched).map(_.scores))

    statsm.foreach { println }
    statsn.foreach { println }
    val res = statsm.zip(statsn).map {
      case (m, n) =>
        (m.missing + n.missing, m.stats.mean - n.stats.mean)
    }

    res.foreach(println)

  }

  def toDouble(s: String): Double = {
    if ("?" == s) Double.NaN else s.toDouble
  }

  def isHeader(line: String): Boolean = line.contains("id_1")

  case class MatchData(id1: Int, id2: Int, scores: Array[Double], matched: Boolean)

  def parser(line: String): MatchData = {
    val p = line.split(',')
    val id1 = p(0).toInt
    val id2 = p(1).toInt
    val scores = p.slice(2, 11).map(toDouble)
    val matched = p(11).toBoolean
    MatchData(id1, id2, scores, matched)

  }

  class NAStatCounter extends Serializable {
    val stats: StatCounter = new StatCounter()

    var missing: Long = 0

    def add(x: Double): NAStatCounter = {
      if (java.lang.Double.isNaN(x)) {
        missing += 1
      } else {
        stats.merge(x)
      }
      this
    }

    def merge(other: NAStatCounter): NAStatCounter = {
      stats.merge(other.stats)
      missing += other.missing
      this
    }
    override def toString = {
      s"stats:${stats.toString} NaN:${missing}"
    }

  }
  object NAStatCounter extends Serializable {

    def apply(x: Double) = new NAStatCounter().add(x)
  }

  def statsWithMissing(rdd: RDD[Array[Double]]): Array[NAStatCounter] = {
    val nastats = rdd.mapPartitions(iter => {
      val nas = iter.next().map(d => NAStatCounter(d))
      iter.foreach(arr => {
        nas.zip(arr).foreach { case (n, d) => n.add(d) }
      })
      Iterator(nas)
    })
    nastats.reduce((n1, n2) => {
      n1.zip(n2).map { case (a, b) => a.merge(b) }
    })

  }

}
