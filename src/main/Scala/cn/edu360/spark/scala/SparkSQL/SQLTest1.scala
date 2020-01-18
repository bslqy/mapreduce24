package cn.edu360.spark.scala.SparkSQL
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}

/***
 *  Spark 2.x sql API
 */
object SQLTest1 {
  def main(args: Array[String]): Unit = {
    // SparkSession是spark2.x SQL 执行入口
    val session: SparkSession = SparkSession.builder().appName("SQLTest1").master("local[*]").getOrCreate()

    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("D:\\UoM\\mapreduce24\\SparkTest\\teacher.txt")

    //整理数据
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })

    //结果类型，其实就是表头，用于描述DataFrame
    val schema : StructType = StructType(
      List(
        StructField("id",LongType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true),
        StructField("fv",DoubleType,true)
      )

    )

    //创建DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD,schema)

    // 过滤条件
    import session.implicits._
    val df2: Dataset[Row] = df.where($"fv" > 65 ).orderBy($"fv" desc, $"age" asc)

    df2.show()
    session.stop()
  }

}
