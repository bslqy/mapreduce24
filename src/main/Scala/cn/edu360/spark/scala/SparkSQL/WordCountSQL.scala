package cn.edu360.spark.scala.SparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object WordCountSQL {

  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark = SparkSession.builder().appName("WordCountSQL").master("local[*]").getOrCreate()

    // 指定以后从哪里读数据, 是lazy,更加智能的RDD
    // Dataset 是分布式数据集,是对RDD的进一步封装,是更加智能的RDD
    // dataset只有一列,默认这列叫value
    val lines: Dataset[String] = spark.read.textFile("D:\\UoM\\mapreduce24\\SparkTest\\wc.txt")

    // 导入隐式转换才能
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    // 注册(虚拟的表) 视图
    words.createTempView("v_wc")

    //执行SQL (Transformation, lazy)
    val result: DataFrame = spark.sql("Select value, COUNT(*) counts from v_wc GROUP BY value")

    result.show()


    spark.stop()

  }

}
