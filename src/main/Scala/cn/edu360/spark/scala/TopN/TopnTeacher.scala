package cn.edu360.spark.scala.TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopnTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopnTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines:RDD[String] = sc.textFile("D:\\UoM\\mapreduce24\\SparkTest\\teacher")
    // http://bigdata.edu360.cn/laozhang


    val names: RDD[String] = lines.map(_.split("/")(3))
    val subjects: RDD[String] = lines.map(_.split("/")(2).split("[.]")(0))

    val namesAndOne: RDD[(String,Int)] = names.map((_,1))

    val reduce: RDD[(String, Int)] = namesAndOne.reduceByKey(_+_)

    val sorted: RDD[(String, Int)] = reduce.sortBy(_._2,false)

    val result: Array[(String, Int)] = sorted.collect()


    println(result.toBuffer)

    sc.stop()




  }

}
