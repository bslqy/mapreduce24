package cn.edu360.spark.scala.SparkSQL

import cn.edu360.spark.scala.broadcastVariable.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IpLocationSQL2 {

  /***
   *
   * 缓存所有的分区规则
   * @param args
   */

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("IpLocationSQL2").master("local[*]").getOrCreate()

    import spark.implicits._

    // 从HDFS读取数据
    val rulesAndLines: Dataset[String] = spark.read.textFile(args(0))

    //整理IP规则数据 (计算部分IP切分规则)
    val rulesDataset = rulesAndLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val start = fields(2).toLong
      val end = fields(3).toLong
      val province = fields(6)
      (start, end, province)
    })

    //收集Ip规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = rulesDataset.collect()
    // 广播(必须使用sparkContext)
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)


    //创建RDD,读取访问日志
    val accesslines: Dataset[String] = spark.read.textFile(args(1))

    val ipDataFrame: DataFrame = accesslines.map(line => {
      val fields = line.split("[|]")
      val ip = fields(1)
      //将IP换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }
    ).toDF("ip_num")


    //创建views
    ipDataFrame.createTempView("v_ips")

    // 定义一个自定义函数 (UDF), 并注册
    spark.udf.register("ip2Province",(ipNum: Long) => {
      // 查找ip规则 (事先已经广播了,已经在Executor中了)
      //使用广播变量引用就可以获得
      val IpRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 根据IP地址对应的十进制查找
      val index: Int = MyUtils.binarySearch(IpRulesInExecutor,ipNum)
      var province: String  = "Unknown"
      if(index != -1){
        province = IpRulesInExecutor(index)._3
      }
      province
    })

    // 写SQL计算. 但是需要自定义函数
    val r = spark.sql("SELECT ip2Province(ip_num) province, COUNT(*) counts FROM v_ips GROUP BY province ORDER BY counts DESC")
    r.show()
    spark.stop()

  }

}
