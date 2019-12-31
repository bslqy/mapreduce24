package ScalaLesson

object PartialFunction {

  /***
   * 被包在花括号内没有match 的一组case 语句是一个偏函数，它是PartialFunction[A, B]的一个
   * 实例，A 代表参数类型，B 代表返回类型，常用作输入模式匹配。
   *
   * @return
   */
  def func1: PartialFunction[String, Int] = {
    case "one" => 1
    case "two" => 2
    case _ => -1
  }
  def func2(num: String) : Int = num match {
    case "one" => 1
    case "two" => 2
    case _ => -1
  }
  def main(args: Array[String]) {
    println (func1 ("one"))
    println (func2 ("one"))
  }

}
