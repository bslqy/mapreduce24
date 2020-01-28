package cn.edu360.spark.scala.Streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulKafkaWordCount {
  /***
   * 第一个参数： 聚合的key
   * 第二个参数：当前批次产生的次数（从多个分区内的，需要累加获得最终数字）
   * 第三个参数：初始值或累加值的中间结果
   */
  val updateFunc = (iter:Iterator[(String,Seq[Int],Option[Int])]) => {
//    iter.map(t => (t._1,t._2.sum + t._3.getOrElse(0)))
    iter.map{case(x,y,z) => (x,y.sum + z.getOrElse(0))}

  }
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StatefulKafkaWordCount").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

    //如果需要使用历史数据（累加），就要把最终结果保存起来.生产环境指定分布式文件系统

    ssc.checkpoint("")


    //创建DStream,需要KafkaDSTream
    val zkQuorum = "hdp-01:2181,hdp-02:2181,hdp-03:2181,hdp-04:2181"
    val groupID = "gl"
    val topic = Map[String,Int]("xiaoniuabc" -> 1)

    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupID,topic)

    // Kafka的ReceiverInputDStream[(String,String])里面装的是一个元组
    // （key是写入的key，value是实际写入的内容）
    val lines: DStream[String] = data.map(_._2)
    //对DStream进行操作，你操作这个抽象，就像本地集合一样
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和1组合一次
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //打印结果
    reduced.print()
    //要启动sparkStreaming程序
    ssc.start()
    //优雅的推出
    ssc.awaitTermination()
  }

}
