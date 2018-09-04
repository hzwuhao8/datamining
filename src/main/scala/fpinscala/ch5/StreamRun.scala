package fpinscala.ch5

object StreamRun  extends App {
  println("Welcome to the Scala worksheet")
	def  ones: Stream[Int] = Stream.cons(1,ones)
	val two = ones.take(20)
	println(two)
	def  intList(n: Int) : Stream[Int] = Stream.cons(  n  , intList(n+1))
	println( intList(1).take(10).toList )
	val v2 = intList(2)
	println(v2)
}