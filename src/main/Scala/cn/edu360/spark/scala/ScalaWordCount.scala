package cn.edu360.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ScalaWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    // 创建Spark执行入口
    val sc = new SparkContext(conf);
    //指定从哪里读取数据读取RDD
    val lines: RDD[String] = sc.textFile(args(0))
    // 切分压平
    val words:RDD[String] = lines.flatMap(_.split(" "))
    // 将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))
    // 按key进行聚合
    val reduce:RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)
    // 排序
    val sorted: RDD[(String, Int)] = reduce.sortBy(_._2,false)
    // 将结果保存到HDFS中
    sorted.saveAsTextFile(args(1))
    // 释放资源
    sc.stop()




    


  }


}
