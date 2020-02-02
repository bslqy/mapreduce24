package cn.dmp.tools

import cn.dmp.bean.Log
import cn.dmp.cn.dmp.utils.SchemaUtils
import cn.dmp.utils.NBF
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

/***
 * 将原始日式文件转换成parquet文件格式
 * 采用snappy压缩格式
 * 自定义类去实现
 */
object Bzip2ParquetV2 {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if(args.length !=2){
      println(
        """
          |cn.dmp.tools.Bzip2Parquet
          |参数.
          |logtInputPath
          |ResultOutputPath
          |""".stripMargin)
      sys.exit()
    }

    // 1 接受程序的参数
    val Array(logInputPath,resultOutputPath) =args

    //2 创建SparkContext
    val sparkConf  = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer","org.apache.spark.serializer.KyroSerializer")
    //注册自定义类的序列化方式
    sparkConf.registerKryoClasses(Array(classOf[Log]))


    val session: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    //3 读取日志数据
    val rawdata = session.sparkContext.textFile(logInputPath)

    //4 根据业务需求对数据进行处理 ETL (limit表示不管如何切到最后，没有数据也切)
    val rowData: RDD[Log] = rawdata.map(line => line.split(",", line.length))
      .filter(_.length >= 85)
      .map(arr => {
        //调用Log伴生对象的静态方法
        Log(arr)
      })


    //5 讲结果存储到本地磁盘
    val dataFrame = session.createDataFrame(rowData)
    dataFrame.write.partitionBy("provincename","cityname").parquet(resultOutputPath)
    //6 关闭sc
    session.stop()

  }

}
