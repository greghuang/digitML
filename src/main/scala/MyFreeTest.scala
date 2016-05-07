/**
  * Created by greghuang on 5/7/16.
  */
object MyFreeTest extends App {
  val a = Array(1 to 10, 11 to 20)
  val a1 = Array(1, 3, 5, 7, 9)
  val a2 = Array(2, 4, 6, 8, 10)
  val a3 = Seq(a1, a2)

  a1.foreach(println)
  a2.foreach(println)

  a3.flatten.foreach(println)
  val pf: PartialFunction[Int, Int] = {
    case x if x > 5 => x
    case _ => 0
  }
  println
  def isValid(x: Int) = if (x > 5) x else 0

  val a4 = a3.map(x =>
    x.map(isValid(_))
  )
  a4.flatten.foreach(println)
}
