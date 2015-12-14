package ch2

object wty02 {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet

  def r1(x: Int) = {
    if (x <= 0) 0
    else if (x <= 17) 2.2 * x
    else if (x <= 31) 2.2 * 17 + (x - 17) * 2.9
    else 37.4 + 40.6 + 3.6 * (x - 31)
  }                                               //> r1: (x: Int)Double

  type S = (Int, Double)

  def r2(x: Int, s: List[S]) = {

  }                                               //> r2: (x: Int, s: List[ch2.wty02.S])Unit
  
  def split(x: Int, a: Int, b: Int) = {
    if (x < a) 0
    else if (x <= b) x - a
    else b - a
  }                                               //> split: (x: Int, a: Int, b: Int)Int

val list = (0 to(100,10)).toList.sliding(2) .toList
                                                  //> list  : List[List[Int]] = List(List(0, 10), List(10, 20), List(20, 30), List
                                                  //| (30, 40), List(40, 50), List(50, 60), List(60, 70), List(70, 80), List(80, 9
                                                  //| 0), List(90, 100))
 val listfunc =  list.map( l => split(_:Int , l.head,l.tail.head))
                                                  //> listfunc  : List[Int => Int] = List(<function1>, <function1>, <function1>, <
                                                  //| function1>, <function1>, <function1>, <function1>, <function1>, <function1>,
                                                  //|  <function1>)
val splitdata = listfunc.map(_(85))               //> splitdata  : List[Int] = List(10, 10, 10, 10, 10, 10, 10, 10, 5, 0)

splitdata.sum                                     //> res0: Int = 85

  def r(x: Int, p1: S, p2: S, p3: S) = {
    val s1 = p1._1
    val x1 = p1._2
    val s2 = p2._1
    val x2 = p2._2
    val x3 = p3._2
    if (x <= 0) 0
    else if (x <= s1) x1 * x
    else if (x <= s2) s1 * x1 + (x - s1) * x2
    else s1 * x1 + (s2 - s1) * x2 + x3 * (x - s2)
  }                                               //> r: (x: Int, p1: ch2.wty02.S, p2: ch2.wty02.S, p3: ch2.wty02.S)Double

  val s1: S = (17, 2.2)                           //> s1  : ch2.wty02.S = (17,2.2)

  val f1 = r(_: Int, (17, 2.2), (31, 2.9), (Int.MaxValue, 3.6))
                                                  //> f1  : Int => Double = <function1>

  f1(20)                                          //> res1: Double = 46.10000000000001

  f1(100)                                         //> res2: Double = 326.4
  r1(100)                                         //> res3: Double = 326.4

  f1(100) == r1(100)                              //> res4: Boolean = true

  val f2 = r(_: Int, (18 * 2, 2.9), (30 * 2, 3.85), (Int.MaxValue, 6.7))
                                                  //> f2  : Int => Double = <function1>

  f2(19)                                          //> res5: Double = 55.1

  val f3 = r(_: Int, (23, 3.1), (40, 3.72), (Int.MaxValue, 4.65))
                                                  //> f3  : Int => Double = <function1>
  f3(10)                                          //> res6: Double = 31.0

  val f4 = r(_: Int, (Int.MaxValue, 1), (0, 0), (0, 0))
                                                  //> f4  : Int => Double = <function1>

  f4(0) == 0                                      //> res7: Boolean = true
  f4(10) == 10.0                                  //> res8: Boolean = true

}