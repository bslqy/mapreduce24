package ScalaLesson

object Currying {

  // 我们看下这个方法的定义, 求2 个数的和
  def add(x: Int, y: Int) = x + y
  // 那么我们应用的时候，应该是这样用：add(1,2)
  // 现在我们把这个函数变一下形：
  def add2(x:Int)(y:Int) = x + y
  // 那么我们应用的时候，应该是这样用：add(1)(2),最后结果都一样是3，这种方式（过程）就叫柯里化。经过柯里化之后，函数的通用性有所降低，但是适用性有所提高。
  // 分析下其演变过程
  def add3(x: Int) = (y: Int) => x + y
  // (y: Int) => x + y 为一个匿名函数, 也就意味着add 方法的返回值为一个匿名函数
  // 那么现在的调用过程为
//  val result = add(2)
  //  ////  val sum1 = result (3)
  //  ////  val sum2 = result (8)

  def main(args: Array[String]): Unit = {

  }
}
