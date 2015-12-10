package introdm.ch4

import ml.ch3.ID3

object P432 {

  val dataSet = IndexedSeq(
    IndexedSeq('是, '单身, 125, 'no),
    IndexedSeq('否, '已婚, 100, 'no),
    IndexedSeq('否, '单身, 70, 'no),
    IndexedSeq('是, '已婚, 120, 'no),
    IndexedSeq('否, '离异, 95, 'yes),
    IndexedSeq('否, '已婚, 60, 'no),
    IndexedSeq('是, '离异, 220, 'no),
    IndexedSeq('否, '单身, 85, 'yes),
    IndexedSeq('否, '已婚, 75, 'no),
    IndexedSeq('否, '单身, 90, 'yes))
  val labels = IndexedSeq('有房者, '婚姻状况, '年收入)
  def main(args: Array[String]) = {

    val r = ID3.createTree(dataSet, labels)
    println(r)
  }

  val dataSet2 = dataSet.map( row => Data(Some( row(0).asInstanceOf[Symbol].toString()),
      row(1).asInstanceOf[Symbol].toString,
      row(2).asInstanceOf[Int],
      row(3).asInstanceOf[Symbol].toString) )
      
  
  case class Data(house: Option[String], wenxin: String ,salary: Int, label: String)
  
}

