package cn.dmp.report


import cn.dmp.bean.Log
import cn.dmp.utils.RptUtils
import org.apache.spark.{SparkConf, SparkContext}

object AreaAnalysisSparkCore {
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

    val sc = new SparkContext(sparkConf)
    //读取Parquet文件
    sc.textFile(logInputPath)
      .map(_.split(",", -1))
      .filter(_.length >= 85)
      .map(arr => {
        val log = Log(arr)
        val req = RptUtils.caculateReq(log.requestmode, log.processnode)
        val rtb = RptUtils.caculateRtb(log.iseffective, log.isbilling,log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
        val showClick = RptUtils.caculateShowClick(log.requestmode, log.iseffective)

        ((log.provincename, log.cityname), req ++ rtb ++ showClick)
        // ((省，地市)，[媒体，渠道，操作系统，网络类型,...，List 9个指标数据])
      }).reduceByKey((list1, list2) => {
      // value进行计算
      // zip ((A1,B1),(A2,B2),..(A9,B9)) => ((A1+B1),...(A9+B9))
      list1.zip(list2).map(t => t._1 + t._2)
      // 省，市，指标1,指标2
    }).map(t => t._1._1+","+t._1._2+","+t._2.mkString(","))
      .saveAsTextFile(resultOutputPath)



    sc.stop()
  }
}
