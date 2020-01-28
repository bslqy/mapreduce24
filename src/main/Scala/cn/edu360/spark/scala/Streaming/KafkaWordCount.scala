package cn.edu360.spark.scala.Streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(conf,Seconds(5))

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
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //打印结果
    reduced.print()

    //要启动sparkStreaming程序
    ssc.start()
    //优雅的推出
    ssc.awaitTermination()



  }

}
