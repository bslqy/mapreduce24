package cn.edu360.spark.scala.Streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

object IpUtils {
  def broadcastIpRules(ssc: StreamingContext, ipRulesPath: String) =
  {
    val sc = ssc.sparkContext
    // 从HDFS读取数据
    val rulesAndLines: RDD[String] = sc.textFile(ipRulesPath)

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

    broadcastRef
  }



}
