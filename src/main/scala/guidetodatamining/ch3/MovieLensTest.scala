package guidetodatamining.ch3

import guidetodatamining.ch2.Recommender.UserMap

/**
 * 数据集 u1.base 训练 ， u1.test 进行测试
 *
 */
object MovieLensTest extends util.Log with MovieData {

  def main(args: Array[String]) {
    val data = loadR(path + "u1.base")
    val testData = loadR(path + "u1.test")
    val item = loadM()
    log.info(s"data.size=${data.size}")
    slopeone(data, testData)
    
    cos(data,testData)
    
  }

  def cos(data: UserMap, testData: UserMap) {
    val r = new CosRecommend(data)
    val sdev = r.sdev
    log.debug(s"sdev= ${sdev.take(1)}")

    val basemovies = data("1")
    val movies = testData("1")
    log.debug(s"movies.size=${movies.size}")
    val deltaSeq = movies.par.map {
      case (k, v) =>
        val estimate = r.pui(basemovies, k)
        val delta = estimate - v
        delta
    }
    val (max, min) = (deltaSeq.max, deltaSeq.min)

    val sdev2 = Math.sqrt(deltaSeq.map(x => x * x).sum)
    println(s"cos  max=${max},min=${min},dev = ${sdev2}")
  }

  def slopeone(data: UserMap, testData: UserMap) {
    val r = new SlopeOne(data)
    val sdev = r.devs
    log.debug(s"sdev= ${sdev.take(1)}")

    val basemovies = data("1")
    val movies = testData("1")
    log.debug(s"movies.size=${movies.size}")
    val deltaSeq = movies.par.map {
      case (k, v) =>
        val estimate = r.pui(basemovies, k)
        val delta = estimate - v
        delta
    }
    val (max, min) = (deltaSeq.max, deltaSeq.min)

    val sdev2 = Math.sqrt(deltaSeq.map(x => x * x).sum)
    println(s"slopeone max=${max},min=${min},dev = ${sdev2}")
  }
}