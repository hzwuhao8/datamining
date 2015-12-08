package ml.ch3

object ID3 extends App {

  implicit class PrettyPrintMap[K, V](val map: Map[K, V]) {
    def prettyPrint: PrettyPrintMap[K, V] = this

    override def toString: String = {
      val valuesString = toStringLines.mkString("\n")

      "Map (\n" + valuesString + "\n)"
    }

    def toStringLines = {
      map
        .flatMap{ case (k, v) => keyValueToString(k, v)}
        .map(indentLine(_))
    }

    def keyValueToString(key: K, value: V): Iterable[String] = {
      value match {
        case v: Map[_, _] => Iterable(key + " -> Map (") ++ v.prettyPrint.toStringLines ++ Iterable(")")
        case x => Iterable(key + " -> " + x.toString)
      }
    }

    def indentLine(line: String): String = {
      "\t" + line
    }
  }
  
  def calcShannoneEnt(dataSet: IndexedSeq[IndexedSeq[Any]]): Double = {
    val log102 = Math.log10(2)
    val total = dataSet.size
    val g = dataSet.groupBy { x => x.last }.map(kv => kv._1 -> kv._2.size)

    val t = g.map(kv => kv._2.toDouble / total).map(x => x * Math.log10(x)).map(x => x / log102)
    -(t.sum)

  }

  def splitDataSet(dataSet: IndexedSeq[IndexedSeq[Any]], axis: Int, value: Any): IndexedSeq[IndexedSeq[Any]] = {
    dataSet.filter(row => row(axis) == value).map { row =>

      val (p1, p2) = row.splitAt(axis)
      p1 ++ p2.drop(1)
    }
  }

  def chooseBestFeaturetoSplit(dataSet: IndexedSeq[IndexedSeq[Any]]): (Int, Double) = {
    val numberFeatures = dataSet.head.size - 1
    val baseEntropy = calcShannoneEnt(dataSet)

    val d2 = for (i <- 0 until numberFeatures) yield {
      val uniqueValus = dataSet.map(row => row(i)).distinct

      val d = for (v <- uniqueValus) yield {
        val subDataSet = splitDataSet(dataSet, i, v)
        val prob = subDataSet.size.toDouble / dataSet.size
        prob * calcShannoneEnt(subDataSet)
      }
      val newEntropy = d.sum
      val infoGain = baseEntropy - newEntropy
      (i, infoGain)
    }
    println(d2)
    d2.sortBy(_._2).reverse.head

  }

  def major[A](col: IndexedSeq[A]): A = {
    val t1 = col.groupBy { x => x }.map(kv => kv._1 -> kv._2.size).toList
    val h = t1.sortBy(kv => kv._2).reverse.head
    h._1
  }

  def cols(dataSet: IndexedSeq[IndexedSeq[Any]], index: Int): IndexedSeq[Any] = {
    dataSet.map(row => row(index))
  }
  def colsSize(dataSet: IndexedSeq[IndexedSeq[Any]]): Int = dataSet.head.size

  def classListVal(dataSet: IndexedSeq[IndexedSeq[Any]]) = cols(dataSet, colsSize(dataSet) - 1)

  def createTree(dataSet: IndexedSeq[IndexedSeq[Any]], labels: IndexedSeq[Symbol]): Any = {
    println("\n\n" + "-" * 60)
    println(s"dataSet=${dataSet}")
    println(s"labels=${labels}")
    val classList = classListVal(dataSet)
    println(s"classList=${classList}")

    val res = if (classList.forall { x => x == classList.head }) {
      classList.head
    } else if (dataSet.head.size == 1) {
      major(classList)
    } else {
      
      val bestFeat = chooseBestFeaturetoSplit(dataSet)
      val bestFeatLabel = labels(bestFeat._1)
      println(s"bestFeatLabel=${bestFeatLabel}")
      val subLabel = labels.filter(_ != bestFeatLabel)
      val featValues = dataSet.map(row => row(bestFeat._1))
      val uniqueVals = featValues.distinct
      println(s"uniqueVals=${uniqueVals}")
      val x = uniqueVals.map { value =>
        val k = value
        val v = createTree(splitDataSet(dataSet, bestFeat._1, value), subLabel)
        println(s"kv=${k -> v}")
        k -> v
      }
      val myTree = Map(bestFeatLabel -> x.toMap)
      myTree
    }
    println(res)
    res

  }

  def classify(inputTree2: Any, labels: IndexedSeq[Symbol], testVec: IndexedSeq[Any]): Symbol = {
    inputTree2 match {
      case inputTree: Map[_, _] =>
        println(inputTree)
        val firstStr = inputTree.keys.head
        val secondDict = inputTree(firstStr)
        val featIndex = labels.indexOf(firstStr)
        println(s"firstStr=${firstStr}")
        println(s"secondDict=${secondDict}")
        println(s"featIndex=${featIndex}")
        secondDict match {
          case dict: Map[_, _] =>
            val sq = for (key <- dict.keys) yield {
              val classLabel = if (testVec(featIndex) == key) {
                dict(key) match {
                  case dd: Map[_, _] => classify(dd, labels, testVec)
                  case x: Symbol     => x
                }
              } else {
                'UNKNOWN
              }
              classLabel
            }
            sq.filterNot(_ == 'UNKNOWN).headOption.getOrElse('UNKNOWN)

          case x: Symbol => x
        }
    }

  }

  val dataSet = IndexedSeq(

    IndexedSeq(1, 1, 'yes),
    IndexedSeq(1, 1, 'yes),
    IndexedSeq(1, 0, 'no),
    IndexedSeq(0, 1, 'no),
    IndexedSeq(0, 1, 'no))
  val s1 = calcShannoneEnt(dataSet)
  println(s"s1=${s1}")

  val s2 = calcShannoneEnt(dataSet.drop(1).+:(IndexedSeq(1, 1, 'maybe)))
  println(s"s2=${s2}")
  println(splitDataSet(dataSet, 0, 1))
  println(splitDataSet(dataSet, 0, 0))

  println(chooseBestFeaturetoSplit(dataSet))

  println(major(dataSet.map(row => row.last)))

  val labels = IndexedSeq('no_surfacing, 'flippers)

  val r = createTree(dataSet, labels)
  println(r)
  val c1 = r match {
    case inputTree: Map[_, _] =>
      val dataSet = IndexedSeq(
        IndexedSeq(1, 1),
        IndexedSeq(0, 1),
        IndexedSeq(1, 0),
        IndexedSeq(0, 0),
        IndexedSeq(2, 0),
        IndexedSeq(0, 2),
        IndexedSeq(2, 1),
        IndexedSeq(1, 2),
        IndexedSeq(2, 2))
      dataSet.map { row => row -> classify(inputTree, labels, row) }

  }
  println(s"c1=${c1.mkString("\n")}")

}