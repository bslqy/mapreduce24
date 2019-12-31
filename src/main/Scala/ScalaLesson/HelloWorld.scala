package ScalaLesson

import java.util.Date

import org.junit.Test

object HelloWorld {


  def add(a:Int,b:Int): Int = { a + b }

  val add2 = (a:Int,b:Int) => a + b

  val add3 :(Int,Int) => Int = (a,b) => a + b

  // 4.14 高阶函数

  // 高阶函数: 将其他函数作为参数或其结果是函数的函数
  // 定义一个方法, 参数为带一个整型参数返回值为整型的函数f 和一个整型参数v, 返回值为一个函数
  def apply(f:Int => String, v:Int): Unit = f(v)

  def layout(x:Int): String = "[" + x.toString() +"]"

// 4.15 部分参数应用函数



  // 定义个输出的方法, 参数为date, message
  def log(date: Date, message: String) = {
    println (s"$date, $message")
  }
  val date = new Date()
  // 调用log 的时候, 传递了一个具体的时间参数, message 为待定参数
  // logBoundDate 成了一个新的函数, 只有log 的部分参数(message)
  val logBoundDate : (String) => Unit = log (date , _: String)
  // 调用logBoundDate 的时候, 只需要传递待传的message 参数即可
  logBoundDate ("fuck jerry ")
  logBoundDate ("fuck 涛涛")

  //4.16 柯里化(Currying)


  def main(args: Array[String]): Unit = {
    println(add(1,2))

    println (apply (layout , 10))
  }

}
