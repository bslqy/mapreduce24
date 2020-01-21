package cn.edu360.spark.scala.SparkSQL

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession,Dataset,Row}

object TopnTeacher {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TopnTeacher").master("local[*]").getOrCreate()
    val lines: RDD[String] = spark.sparkContext.textFile(args(0))

    import spark.implicits._

    val df:DataFrame = lines.map(line => {
      val tIndex = line.lastIndexOf("/")+1
      val teacher = line.substring(tIndex)
      val host = new URL(line).getHost
      //学科的Index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0,sIndex)
      (subject,teacher)
    }).toDF("subject","teacher")


    df.createTempView("v_sub_teacher")

    val temp1: DataFrame = spark.sql("SELECT subject,teacher,count(*) as counts FROM v_sub_teacher GROUP BY subject,teacher")

    //求每个学科下最受欢迎老师的topN
    temp1.createTempView("v_temp_sub_teacher_counts")

    //局部 + 全局排序
//    val temp2 = spark.sql("SELECT subject,teacher, counts, row_number() OVER(order by counts desc) gr, row_number() OVER(PARTITION BY subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts")
    val Topn = 2
    val temp2 = spark.sql(s"SELECT *, row_number() over(order by counts desc) g_rk FROM (SELECT subject,teacher, counts, row_number() OVER(order by counts desc) gr, row_number() OVER(PARTITION BY subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $Topn")


    temp2.show()

    spark.stop()

  }

}
