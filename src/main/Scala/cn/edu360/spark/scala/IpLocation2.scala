package cn.edu360.spark.scala

import java.sql.{Connection, Driver, DriverManager}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation2 {

  /***
   *
   * 缓存所有的分区规则
   * @param args
   */

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation2").setMaster("local[4]")

    val sc = new SparkContext(conf)


    // 从HDFS读取数据
    val rulesAndLines: RDD[String] = sc.textFile(args(0))

    //整理IP规则数据 (计算部分IP切分规则)
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesAndLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val start = fields(2).toLong
      val end = fields(3).toLong
      val province = fields(6)
      (start, end, province)
    })

    // 将分散在多个Excutor上的部分IP规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()
    //将Driver端的数据广播到Executor中
    //调用sc上的广播方法
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)


    //创建RDD,读取访问日志
    val accesslines: RDD[String] = sc.textFile(args(1))


    // 这个函数是在那个端定义的? (Driver).  但是这段业务逻辑要在Executor端执行
    val func = (line:String) =>{
      val fields = line.split("[|]")
      val ip = fields(1)
      //将IP换成十进制
      val ipNum = MyUtils.ip2Long(ip)

      // 通过Driver端的引用获取到Executor中的广播变量 (ip切分规则)!!!!
      // (该函数中的代码是在Executor中调用别的执行的)
      // Driver 中的广播变量引用是如何跑到Executor中的呢?
      //Task是在Driver端生成的,广播变量的引用是伴随着Task被发送到Executor中的
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

    // 一次拿出一个分区,一个分区用一个连接,可以将一个分区中的多条数据写完再释放jdbc连接,这样更节省资源
    reduced.foreachPartition(it => {
      val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata")
      val pstm = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")
      it.foreach( tp => {
        pstm.setString(1,tp._1)
        pstm.setInt(2,tp._2)
        pstm.executeUpdate()
      })
      pstm.close()
      conn.close()
    })







  }

}
