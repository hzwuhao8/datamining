package ml.ch3

object ID3 extends App {

  def calcShannoneEnt(dataSet: List[List[Any]]): Double = {
    val log102 = Math.log10(2)
    val total = dataSet.size
    val g = dataSet.groupBy { x => x.last }.map(kv => kv._1 -> kv._2.size)

    val t = g.map(kv => kv._2.toDouble / total).map(x => x * Math.log10(x)).map(x => x / log102)
    -(t.sum)

  }

  def splitDataSet(dataSet: List[List[Any]], axis: Int, value: Any): List[List[Any]] = {
    dataSet.filter(row => row(axis) == value).map { row =>
      val (p1, p2) = row.splitAt(axis)
      p1 ::: p2.tail
    }
  }

  def chooseBsetFeaturetoSplit(dataSet: List[List[Any]]): (Int, Double) = {
    val numberFeatures = dataSet.head.size - 1
    val baseEntropy = calcShannoneEnt(dataSet)
    var bsetInfoGain = 0.0 ;
    var bestFeature = - 1
    val d2  =  for( i <-  0 until numberFeatures) yield{
      val uniqueValus = dataSet.map( row => row(i)).distinct
      
      val d = for( v <- uniqueValus) yield{
        val subDataSet = splitDataSet(dataSet , i, v)
        val prob = subDataSet.size.toDouble  / dataSet.size
        prob * calcShannoneEnt(subDataSet)
      }
      val newEntropy = d.sum
      val infoGain = baseEntropy - newEntropy
      (i,infoGain)
    }
    println(d2)
    d2.sortBy(_._2).reverse.head
    
  }

  def major[A]( col: List[A]): A = {
    val t1 = col.groupBy { x => x }.map( kv => kv._1 -> kv._2.size).toList
    val h =  t1.sortBy( kv => kv._2).reverse.head
    h._1
  }
  
  val dataSet = List(

    List(1, 1, 'yes),
    List(1, 1, 'yes),
    List(1, 0, 'no),
    List(0, 1, 'no),
    List(0, 1, 'no))
  val s1 = calcShannoneEnt(dataSet)
  println(s"s1=${s1}")

  val s2 = calcShannoneEnt(List(1, 1, 'maybe) :: dataSet.tail)
  println(s"s2=${s2}")
  println(splitDataSet(dataSet, 0, 1))
  println(splitDataSet(dataSet, 0, 0))
  
  println(chooseBsetFeaturetoSplit(dataSet))
  
}