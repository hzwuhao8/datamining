package ch2

object wty {
import scala.BigDecimal
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet

  def f1(a: Int, b: Int) = {
    val x = a - b
    val y = if (x <= 0) -x
    else x
    (y, r(y))
  }                                               //> f1: (a: Int, b: Int)(Int, Double)

  def r(x: Int) = {
    if (x <= 0) 0
    else if (x <= 17) 2.2 * x
    else if (x <= 31) 2.2 * 17 + (x - 17) * 2.9
    else 37.4 + 40.6 + 3.6 * (x - 31)
  }                                               //> r: (x: Int)Double
  r(8)                                            //> res0: Double = 17.6

  f1(587, 607)                                    //> res1: (Int, Double) = (20,46.10000000000001)

  def g(x: Int) = {
    val S1 = 17
    val S2 = 31

    val a1 = if (x <= S1) x else S1
    val a2 = if (x <= S1) 0 else if (x > S1 && x <= S2) x - S1 else S2- S1
    val a3 = if (x <= S2) 0 else if (x > S2) x - S2 else 0
    List(a1, a2, a3)

  }                                               //> g: (x: Int)List[Int]
  g(0)                                            //> res2: List[Int] = List(0, 0, 0)
  g(17)                                           //> res3: List[Int] = List(17, 0, 0)
  g(20)                                           //> res4: List[Int] = List(17, 3, 0)
  g(31)                                           //> res5: List[Int] = List(17, 14, 0)
  g(32)                                           //> res6: List[Int] = List(17, 14, 1)
  
  def ff(a: Double) = (x: Int) => BigDecimal(a) * x
                                                  //> ff: (a: Double)Int => scala.math.BigDecimal

  val f1List = List(1.75, 2.3, 3.0).map(a => (a, ff(a)))
                                                  //> f1List  : List[(Double, Int => scala.math.BigDecimal)] = List((1.75,<functio
                                                  //| n1>), (2.3,<function1>), (3.0,<function1>))
  val f2List = List(0.45, 0.6, 0.6).map(a => (a, ff(a)))
                                                  //> f2List  : List[(Double, Int => scala.math.BigDecimal)] = List((0.45,<functio
                                                  //| n1>), (0.6,<function1>), (0.6,<function1>))

  def gg(x: Int) = {
    val lista = g(x)
    val a = lista.zip(f1List).map(kv => (kv._1, kv._2._1, kv._2._2(kv._1)) )
    val b = lista.zip(f2List).map(kv => (kv._1, kv._2._1 ,kv._2._2(kv._1)))

   List (a, b)
  }                                               //> gg: (x: Int)List[List[(Int, Double, scala.math.BigDecimal)]]

  gg(8)                                           //> res7: List[List[(Int, Double, scala.math.BigDecimal)]] = List(List((8,1.75,
                                                  //| 14.00), (0,2.3,0.0), (0,3.0,0.0)), List((8,0.45,3.60), (0,0.6,0.0), (0,0.6,
                                                  //| 0.0)))
  val res = gg(20)                                //> res  : List[List[(Int, Double, scala.math.BigDecimal)]] = List(List((17,1.7
                                                  //| 5,29.75), (3,2.3,6.9), (0,3.0,0.0)), List((17,0.45,7.65), (3,0.6,1.8), (0,0
                                                  //| .6,0.0)))
 res.flatMap( ll => ll.map( row=> row._3) ).sum   //> res8: scala.math.BigDecimal = 46.10
}