package cn.edu360.spark.scala.SparkSQL

import org.apache.spark.sql.{Dataset, Row, SparkSession}


object DataSetWordCount {

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql

    // 创建SparkSession
    val spark = SparkSession.builder().appName("DataSetWordCount").master("local[*]").getOrCreate()

    // 指定以后从哪里读数据, 是lazy,更加智能的RDD
    // Dataset 是分布式数据集,是对RDD的进一步封装,是更加智能的RDD
    // dataset只有一列,默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("D:\\UoM\\mapreduce24\\SparkTest\\wc.txt")

    // 导入隐式转换才能
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //使用DataSet的API (DSL)
    import sql.functions._
    val dataFrame: Dataset[Row] = words.groupBy($"value" as "word").count().sort($"count" desc)

    val counts: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)

    counts.show()

    spark.stop()

  }

}
