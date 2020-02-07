package cn.dmp.report

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class ProCityReport {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 2) {
      println(
        """
          |cn.dmp.report.ProCityReport
          |参数.
          |logtInputPath
          |ResultOutputPath
          |""".stripMargin)
      sys.exit()
    }

    // 1 接受程序的参数
    val Array(logInputPath, resultOutputPath) = args

    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //读取Parquet文件
    val df: DataFrame = session.read.parquet(logInputPath)

    //注册视图
    df.createTempView("log")

    //Query
    val result: DataFrame = session.sql("Select provincename,cityname,count(*) ct from log group by provincename,cityname")

    //判断结果的存储路径是否存在，如果存在则删除 (hdfs 如果存在会报错)
    val configuration: Configuration = session.sparkContext.hadoopConfiguration
    val fs: FileSystem = FileSystem.get(configuration)
    val resultPath: Path = new Path(resultOutputPath)

    if (fs.exists(resultPath)){
      fs.delete(resultPath,true)
    }
    //把结果写入文件
    result.coalesce(1).write.json(resultOutputPath)

    //连接数据库 (默认properties文件名为application.conf)
    //导入typesafe包，加载properties. application.conf -> application.json -> application.properties
    val load: Config = ConfigFactory.load()
    val properties: Properties = new Properties()
    properties.setProperty("user",load.getString("jdbc.user"))
    properties.setProperty("password",load.getString("jdbc.password"))
    result.write.jdbc(load.getString("jdbc.url"),load.getString("jdbc.tableName"),properties)

    session.stop()

  }
}
