package ScalaLesson

object higherOrderFunction{
  // 4.14 高阶函数

  // 高阶函数: 将其他函数作为参数或其结果是函数的函数
  // 定义一个方法, 参数为带一个整型参数返回值为整型的函数f 和一个整型参数v, 返回值为一个函数
  def apply(f:Int => String, v:Int): Unit = f(v)

  def layout(x:Int): String = "[" + x.toString() +"]"

  def main(args: Array[String]): Unit = {
    println(apply(layout,10))
  }

}
