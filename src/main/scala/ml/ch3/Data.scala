package ml.ch3

object Data extends App {
  val txt = """young	myope	no	reduced	no lenses
young	myope	no	normal	soft
young	myope	yes	reduced	no lenses
young	myope	yes	normal	hard
young	hyper	no	reduced	no lenses
young	hyper	no	normal	soft
young	hyper	yes	reduced	no lenses
young	hyper	yes	normal	hard
pre	myope	no	reduced	no lenses
pre	myope	no	normal	soft
pre	myope	yes	reduced	no lenses
pre	myope	yes	normal	hard
pre	hyper	no	reduced	no lenses
pre	hyper	no	normal	soft
pre	hyper	yes	reduced	no lenses
pre	hyper	yes	normal	no lenses
presbyopic	myope	no	reduced	no lenses
presbyopic	myope	no	normal	no lenses
presbyopic	myope	yes	reduced	no lenses
presbyopic	myope	yes	normal	hard
presbyopic	hyper	no	reduced	no lenses
presbyopic	hyper	no	normal	soft
presbyopic	hyper	yes	reduced	no lenses
presbyopic	hyper	yes	normal	no lenses
"""

  val textSeq = txt.split("\n").toIndexedSeq
  val dataSet = textSeq.map(_.split("\t").toIndexedSeq)
  dataSet.foreach(println)
  val labels = IndexedSeq('age, 'prescript, 'astigmatic, 'tearRate)
  /**
   * 将 字符串 转成  double 型的数据
   * 建立一个 索引， 用 id  标识 数据
   */
  def oneCol(index: Int) = {
    dataSet.map(row => row(index)).distinct.sortBy { x => x }.zipWithIndex
  }
  val seqMap = for (i <- 0 to dataSet.head.size - 1) yield {
    val c1 = oneCol(i)
    println(c1)
    c1.map(kv => kv._1 -> kv._2).toMap

  }
  println(seqMap)
  val d2 = dataSet.map { row => row.zipWithIndex.map(r => seqMap(r._2)(r._1)) }
  d2.foreach(println)
  val d3 = d2.map(r => (r.last, r.init))
  println(d3)
  
   
  val d4 = util.DataConvert.convert(dataSet)
  
  println(d4)
  
  val d5 = util.DataConvert.convert( introdm.ch4.P432.dataSet)
  println(d5)
  /**
   * *
   * spakr mllib 中的 例子数据是  一个 稀疏矩阵，
   * 每一行 保存  1 有数据的 列下标 2 数据
   * Array[Int] = Array(10,30,40)
   * Array[Double] = Array(1.0,3.0,4.0)
   * 
   * 表示
   * A[x][10] = 1.0 
   * A[x][20] = 3.0
   * A[x][30] = 4.0
   * 
   * 对数据 建立 索引，
   * 转成 double 型的数据
   * 在 导入到 spark 中
   *
   * val v = Vector((1.0,Vector(2.0, 1.0, 0.0, 1.0)), (2.0,Vector(2.0, 1.0, 0.0, 0.0)), (1.0,Vector(2.0, 1.0, 1.0, 1.0)),
   *  (0.0,Vector(2.0, 1.0, 1.0, 0.0)), (1.0,Vector(2.0, 0.0, 0.0, 1.0)), (2.0,Vector(2.0, 0.0, 0.0, 0.0)),
   *   (1.0,Vector(2.0, 0.0, 1.0, 1.0)), (0.0,Vector(2.0, 0.0, 1.0, 0.0)), (1.0,Vector(0.0, 1.0, 0.0, 1.0)),
   *    (2.0,Vector(0.0, 1.0, 0.0, 0.0)), (1.0,Vector(0.0, 1.0, 1.0, 1.0)), (0.0,Vector(0.0, 1.0, 1.0, 0.0)),
   *    (1.0,Vector(0.0, 0.0, 0.0, 1.0)), (2.0,Vector(0.0, 0.0, 0.0, 0.0)), (1.0,Vector(0.0, 0.0, 1.0, 1.0)),
   *     (1.0,Vector(0.0, 0.0, 1.0, 0.0)), (1.0,Vector(1.0, 1.0, 0.0, 1.0)), (1.0,Vector(1.0, 1.0, 0.0, 0.0)),
   *     (1.0,Vector(1.0, 1.0, 1.0, 1.0)), (0.0,Vector(1.0, 1.0, 1.0, 0.0)), (1.0,Vector(1.0, 0.0, 0.0, 1.0)),
   *      (2.0,Vector(1.0, 0.0, 0.0, 0.0)), (1.0,Vector(1.0, 0.0, 1.0, 1.0)), (1.0,Vector(1.0, 0.0, 1.0, 0.0)))
   *
   * val dd = v.map( row =>  new LabeledPoint(row._1, new DenseVector( row._2.toArray )))
   * val dd2 =  sc.makeRDD(dd1)
   * val model3= DecisionTree.trainClassifier(dd2, 3,categoricalFeaturesInfo, impurity,maxDepth, maxBins)
   *
   *
   * 就可以得到 模型
   *
   *
   * 对于  属性的 标称 值 建立一个索引， 用数字 标识  值
   * 对于 训练数据
   *  属性 值，  值 通过映射 转成  Double 型
   * 训练后得到 模型
   *
   * 需要分类的数据， 也进行相应的转换， 得到 数据， 进行分类， 在进行 逆转换， 得到  结果分类
   *
   *
   *
   *
   *
   */
}