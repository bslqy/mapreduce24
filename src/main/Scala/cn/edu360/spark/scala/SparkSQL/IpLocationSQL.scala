package cn.edu360.spark.scala.SparkSQL

import java.sql.{Connection, DriverManager}

import cn.edu360.spark.scala.broadcastVariable.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object IpLocationSQL {

  /***
   *
   * 缓存所有的分区规则
   * @param args
   */

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("IpLocationSQL").master("local[*]").getOrCreate()

    import spark.implicits._

    // 从HDFS读取数据
    val rulesAndLines: Dataset[String] = spark.read.textFile(args(0))


    //整理IP规则数据 (计算部分IP切分规则)
    val ruleDataFrame: DataFrame = rulesAndLines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val start = fields(2).toLong
      val end = fields(3).toLong
      val province = fields(6)
      (start, end, province)
    }).toDF("start","end","province")


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
    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    // 写SQL计算
    val r = spark.sql("SELECT province, COUNT(*) counts FROM v_ips JOIN v_rules on (ip_num >= start AND ip_num <= end) GROUP BY province")
    r.show()
    spark.stop()

  }

}
