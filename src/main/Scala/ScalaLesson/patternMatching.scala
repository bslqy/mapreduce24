package ScalaLesson

object patternMatching {
  def main(args: Array[String]): Unit = {
    val arr = Array (1, 1, 7, 0, 2,3)
    arr match {
      case Array (0, 2, x, y) => println (x + " " + y)
      case Array (2, 1, 7, y) => println ("only 0 " + y)
      case Array (1, 1, 7, _*) => println ("0 ...") // _* 任意多个
      case _ => println ("something else")
    }

    val tup = (1, 3, 7)
    tup match {
      case (3, x, y) => println (s"hello 123 $x , $y")
      case (z, x, y) => println (s"$z, $x , $y")
      case (_, w, 5) => println (w)
      case _ => println ("else")
    }

    val lst = List (0, 3, 4)
    println (lst .head)
    println (lst .tail)
    lst match {
      case 0 :: Nil => println ("only 0")
      case x :: y :: Nil => println (s"x $x y $y")
      case 0 :: a => println (s"value : $a")
      case _ => println ("something else")
    }


  }
}
