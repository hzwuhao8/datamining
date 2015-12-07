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

  def chooseBestFeaturetoSplit(dataSet: List[List[Any]]): (Int, Double) = {
    val numberFeatures = dataSet.head.size - 1
    val baseEntropy = calcShannoneEnt(dataSet)
  
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
  
  def cols(dataSet: List[List[Any]] , index: Int) :List[Any] = {
    dataSet.map( row => row(index))
  }
  def colsSize(dataSet: List[List[Any]] ) : Int =   dataSet.head.size
  
  def classListVal(dataSet: List[List[Any]]) = cols(dataSet , colsSize(dataSet)-1)
  
  def createTree(dataSet: List[List[Any]], labels: List[Symbol]): Any = {
    println("\n\n" + "-" * 60)
    println(s"dataSet=${dataSet}")
    println(s"labels=${labels}")
    val classList = classListVal(dataSet)
    println(s"classList=${classList}")
    
    val res = if( classList.forall { x => x== classList.head }){
       classList.head
    }else if( dataSet.head.size == 1 ){
       major( classList)
    }else{
      val bestFeat = chooseBestFeaturetoSplit(dataSet)
      val bestFeatLabel = labels(bestFeat._1)
      println(s"bestFeatLabel=${bestFeatLabel}")
      val subLabel =  labels.filter( _ != bestFeatLabel)
      val featValues = dataSet.map(row => row( bestFeat._1))
      val uniqueVals = featValues.distinct
      println(s"uniqueVals=${uniqueVals}")
      val x = uniqueVals.map{ value => 
          val k = value
          val v = createTree(splitDataSet(dataSet, bestFeat._1, value),subLabel)
          println(s"kv=${ k -> v }")
          k->v
      }
      val myTree = Map(bestFeatLabel ->  x )
      myTree
    }
    println(res)
    res 
    
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
  
  println(chooseBestFeaturetoSplit(dataSet))
  
  println( major( dataSet.map( row => row.last)))
  
  val labels = List('no_surfacing, 'flippers)
  
  val r = createTree(dataSet, labels)
  println(r)
  
}