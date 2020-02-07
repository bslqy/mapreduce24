package cn.dmp.tags

import cn.dmp.report.AppAnalysisRddBoradcast.args
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SQLContext, SparkSession}

object Tag4Context {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 2) {
      println(
        """
          |cn.dmp.report.Tag4Context
          |参数.
          |输入路径
          |字典路径
          |停用词库路径
          |输出路径
          |""".stripMargin)
      sys.exit()
    }

    // 1 接受程序的参数
    val Array(inputPath,dictFilePath,stopWordsFilePath, outputPath) = args

    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val session = SparkSession.builder().config(sparkConf).getOrCreate()


    //3 Collect为了收集切片，要不然可能只是部分
    val dictMap: Map[String, String] = sc.textFile(dictFilePath).map(line => {
      val field = line.split("\t", -1)
      (field(4), field(1))
    }).collect().toMap

    // 停用词字典文件
    val stopWordMap: Map[String, Int] = sc.textFile(dictFilePath).map((_,0)).collect().toMap



    // 4 广播字典文件到executor
    val broadcastAppDict: Broadcast[Map[String, String]] = sc.broadcast(dictMap)
    val broadcastStopWordsDict: Broadcast[Map[String, Int]] = sc.broadcast(stopWordMap)

    // 读取日誌的parquet文件
    session.read.parquet(inputPath).where(
      """
        |ime != "" or imeimd5 != "" or imeisha1 !=""
        |idfa != "" or idfamd5 != "" or idfasha1 !=""
        |mac != "" or macmd5 != "" or macsha1 !=""
        |andrioidid != "" or andrioididmd5 != "" or andrioididsha1 !=""
        |openudid != "" or openudidmd5 != "" or openudidsha1 !=""
        |""".stripMargin).map(row => {
        // 把数据传入打标签的类，Tags4Ads 类里面实现打标签接口方法
        Tags4Ads.markeTags(row)


    })

    sc.stop()


  }

}
