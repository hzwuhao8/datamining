package ch2

object Q2 extends App {
  val data = List(13,15,16,16,19,20,20,21,22,22,25,25,25,25,30,33,33,35,
      35,35,35,36,40,45,46,52,70)
  val t = data.sum
  val s = data.size
  val a1 = t.doubleValue()/s
  println(s"sum=${t},size=${s}")
  println(s"均值=${a1}")
  val mid =  if(s%2==0){
    (data(s/2) + data(s/2+1))/2
  }else{
    data((s+1)/2)
  }
  println(s"中位数=${mid }")
  
  val g = data.groupBy { x => x }
  val g2 = g.map(kv => (kv._1 -> kv._2.size)).filter(_._2>1).toList.sortBy( x=> x._2).reverse
  println(s"g2=${g2}")
  if(g2.isEmpty){
    println("没有 众数")
    Nil
  }else{
    val h = g2.head
    val t = h::g2.tail.filter(_._2 == h._2)
    println(s"众数=${t}\t模态=${t.size}")
  }
  val m = (data.min + data.max)/2
  println(s"中列数=${m}")
  val q1 = data( s/4)
  val q3 = data(s*3/4)
  println(s"q1=${q1}\tq3=${q3}")

  println(s"五数概括=${data.min},${q1},${mid},${q3},${data.max}")
  
}