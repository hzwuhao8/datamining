package ml.ch3

import ml.ch3.ID3.PrettyPrintMap

object Lenses extends App {
  import ID3.PrettyPrintMap
  
  val txt="""young	myope	no	reduced	no lenses
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
  val textSeq =  txt.split("\n").toIndexedSeq
  val dataSet = textSeq.map( _.split("\t").toIndexedSeq)
  dataSet.foreach(println)
  val labels = IndexedSeq('age, 'prescript, 'astigmatic, 'tearRate )
  val r = ID3.createTree(dataSet, labels)
  println(r)
  r match{
    case m: Map[_,_] => println(  new PrettyPrintMap(m))
  }
  
}