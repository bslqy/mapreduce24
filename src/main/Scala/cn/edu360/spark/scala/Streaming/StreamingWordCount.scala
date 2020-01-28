package cn.edu360.spark.scala.Streaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("StreamingWordCount")
      .setMaster("local[*]")

    // StreamingContext是对SparkContext的包装，包了一层就增加了实时功能
    // 第二个参数是小批次产生时间的间隔
    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc,Milliseconds(5000))
    // 有了StreamingContext，就可以创建SparkStreaming的抽象Dstream
    // 从一个socket端口中读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.226.11",8888)

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
