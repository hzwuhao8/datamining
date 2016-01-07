package guidetodatamining.ch2

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * 使用 spark 做 协同过滤
 * Rank 100 ->  Mean Squared Error = 0.01926699849307847
 *
 */
object Exam02Spark extends Serializable with BX with util.Log {

  val master = "local[3]"

  val appName = "recommendation"

  def main(args: Array[String]) {
    val books = loadBookDBBook()
    val book2idSeq = books.toSeq.map(_._1).sorted.zipWithIndex
    val book2id = book2idSeq.toMap
    val id2book = book2idSeq.map(x => x._2 -> x._1).toMap

    val users = loadBookDBUser()
    val user2idSeq = users.toSeq.map(_._1).sorted.zipWithIndex
    val user2id = user2idSeq.toMap
    val id2user = user2idSeq.map(x => x._2 -> x._1).toMap

    val data = loadBookDBRating()
    val ratings = data.flatMap {
      case (user, v) =>
        val uidOpt = user2id.get(user)
        v.flatMap {
          case (book, rating) =>
            val bookidOpt = book2id.get(book)
            (uidOpt, bookidOpt) match {
              case (_, None)                 => None
              case (None, _)                 => None
              case (Some(uid), Some(bookid)) => Some(new Rating(uid, bookid, rating))
            }

        }
    }.toSeq
    log.debug(s"data.size=${data.size}, ratings.size=${ratings.size}")

    val conf = new SparkConf().setAppName(appName).setMaster(master)

    val sc = new SparkContext(conf)
    val rdd = sc.makeRDD(ratings)
    val rank = 100
    val numIterations = 10
    val model = ALS.train(rdd, rank, numIterations, 0.01)

    // Evaluate the model on rating data
    val usersProducts = rdd.map {
      case Rating(user, product, rate) =>
        (user, product)
    }

    val predictions =
      model.predict(usersProducts).map {
        case Rating(user, product, rate) =>
          ((user, product), rate)
      }

    val ratesAndPreds = rdd.map {
      case Rating(user, product, rate) =>
        ((user, product), rate)
    }.join(predictions)

    val MSE = ratesAndPreds.map {
      case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
        err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    val userid = "171118"
    val user = user2id(userid)
    log.debug(s"user=${user}, id2user(user)={}", id2user(user))

    val res = model.recommendProducts(user, 5)
    res.map {
      case Rating(uid, bookid, rating) =>
        val book = id2book(bookid)
        (uid, bookid, book, books.getOrElse(book, book) -> rating)
    }.foreach(println)

    {
      /**
       *  "171118" 用户的  评价
       */
      val rdd2 = rdd.filter { case Rating(u, _, _) => u == user }
      val usersProducts = rdd2.map {
        case Rating(user, product, rate) =>
          (user, product)
      }
      val predictions =
        model.predict(usersProducts).map {
          case Rating(user, product, rate) =>
            ((user, product), rate)
        }
      val ratesAndPreds = rdd2.map {
        case Rating(user, product, rate) =>
          ((user, product), rate)
      }.join(predictions)

      val MSE = ratesAndPreds.map {
        case ((user, product), (r1, r2)) =>
          val err = (r1 - r2)
          err * err
      }.mean()
      println("171118 Mean Squared Error = " + MSE)

    }

  }

}