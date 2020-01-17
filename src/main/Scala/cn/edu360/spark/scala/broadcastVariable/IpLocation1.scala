package cn.edu360.spark.scala.broadcastVariable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation1 {

  /***
   *
   * 缓存所有的分区规则
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation1").setMaster("local[4]")

    val sc = new SparkContext(conf)

    // 在Driver端获得全部的IP规则数据 (全部IP规则就在某一台机器上,跟Driver在同一台机器上)
    //全部的IP规则在Driver端了 (在Driver端的内存中了)
    val rules: Array[(Long, Long, String)] = MyUtils.readRules(args(0))

    //将Driver端的数据广播到Executor中
    //调用sc上的广播方法
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    //创建RDD,读取访问日志
    val accesslines: RDD[String] = sc.textFile(args(1))

    // 这个函数是在那个端定义的? (Driver).  但是这段业务逻辑要在Executor端执行
    val func = (line:String) =>{
      val fields = line.split("[|]")
      val ip = fields(1)
      //将IP换成十进制
      val ipNum = MyUtils.ip2Long(ip)

      // 通过Driver端的引用获取到Executor中的广播变量 (ip切分规则)!!!!
      // (该函数中的代码实在Executor中调用别的执行的)
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value

      var province = "未知"
      val index: Int = MyUtils.binarySearch(rulesInExecutor,ipNum)
      if (index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)

    }

    //整理数据
    val provinceAndOne: RDD[(String, Int)] = accesslines.map(func)

    // 聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    //将结果打印
    val r: Array[(String, Int)] = reduced.collect()

    println(r.toBuffer)





  }

}
