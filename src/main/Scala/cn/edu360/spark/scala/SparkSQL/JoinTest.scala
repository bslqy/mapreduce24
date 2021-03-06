package cn.edu360.spark.scala.SparkSQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object JoinTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JoinTest").master("local[*]").getOrCreate()

    import spark.implicits._

    val lines: Dataset[String] = spark.createDataset(List(
      "1,laozhao,china", "2,laoduna,usa"
    ))

    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    })
    val df1: DataFrame = tpDs.toDF("id","name","nation")

    val nations: Dataset[String] = spark.createDataset(List("china,中国","use,美国"))
    val ndataSet = nations.map(l => {
      val fields = l.split(",")
      val englishName = fields(0)
      val chineseName = fields(1)
      (englishName, chineseName)
    }
    )
    val df2: DataFrame = ndataSet.toDF("EnglishName","ChineseName")

    // 第一种,创建视图
//    df1.createTempView("v_users")
//    df2.createTempView("v_nations")
//
//    val result: DataFrame = spark.sql("Select name,ChineseName from v_users JOIN v_nations on nation = EnglishName")
//    result.show()
//    spark.stop()


    // 第二种,DSL
    import org.apache.spark.sql.functions._

    val r: DataFrame = df1.join(df2, $"nation" === $"EnglishName")
    val r2: DataFrame = df1.join(df2, $"nation" === $"EnglishName","left_outer")
    r.show()
    r2.show()
    spark.stop()





  }

}
