package fpinscala.ch5

import Stream._
sealed trait Stream[+A] {
  def headOption: Option[A] = this match {
    case Empty         => None
    case Cons(h, tail) => Some(h())
  }
  def toList: List[A] = this match {
    case Empty         => Nil
    case Cons(h, tail) => h() :: tail().toList
  }
  def take(n: Int): Stream[A] = this match {

    case Cons(h, tail) if n > 1  => cons(h(), tail().take(n - 1))
    case Cons(h, tail) if n == 1 => cons(h(), empty)
    case _                       => empty
  }

  def takeWhile(f: A => Boolean): Stream[A] = this match {
    case Cons(h, tail) if (f(h())) => cons(h(), tail() takeWhile (f))
    case _                         => empty
  }

  @annotation.tailrec
  final def drop(n: Int): Stream[A] = this match {
    case Cons(h, tail) if n > 0 => tail().drop(n - 1)
    case _                      => this
  }

  def exists(p: A => Boolean): Boolean = this match {
    case Cons(h, t) => p(h()) || t().exists(p)
    case _          => false
  }

  def foldRight[B](z: => B)(f: (A, => B) => B): B = this match {
    case Cons(h, t) => f(h(), t().foldRight(z)(f))
    case _          => z
  }

  def exists2(p: A => Boolean): Boolean = foldRight(false)((a, b) => p(a) || b)

  def forAll(p: A => Boolean): Boolean = foldRight(true)((a, b) => p(a) && b)
  
  def takeWhile2( p: A => Boolean): Stream[A] = foldRight(empty[A]){(a,b) =>
    if(p(a)){
      cons(  a, b  )
    }else{
      b
    }
      
  }
}

case object Empty extends Stream[Nothing]

case class Cons[+A](h: () => A, t: () => Stream[A]) extends Stream[A]

object Stream {
  def cons[A](hd: => A, tl: => Stream[A]): Stream[A] = {
    lazy val head = hd
    lazy val tail = tl
    Cons(() => hd, () => tail)
  }

  def empty[A]: Stream[A] = Empty

  def apply[A](as: A*): Stream[A] =
    if (as.isEmpty) empty else cons(as.head, apply(as.tail: _*))

}