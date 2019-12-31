package ScalaLesson

import java.util.Date

object PartiallyAppliedFunction {
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


}
