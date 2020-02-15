package cn.dmp.report

import cn.dmp.bean.Log
import cn.dmp.utils.{JedisPool, RptUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object AppAnalysisRddBoradcast extends App {

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
    val Array(appDictInputPath, resultOutputPath) = args

    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

  //3 Collect为了收集切片，要不然可能只是部分
   val dictMap: Map[String, String] = sc.textFile(appDictInputPath).map(line => {
    val field = line.split("\t", -1)
    (field(4), field(1))
  }).collect().toMap

  // 4 广播字典文件到executor
  private val broadcastRef:  Broadcast[Map[String, String]] = sc.broadcast(dictMap)

    sc.textFile(appDictInputPath).map(line =>{
      line.split("\t", -1)
    })
      .filter(_.length >=85)
      .map(Log(_))
    .filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
    .map(log => {
      var newAppname = log.appname
      if("".equals(newAppname)){
        newAppname = broadcastRef.value.getOrElse(log.appid,"未知")
      }

      val req = RptUtils.calculateReq(log.requestmode, log.processnode)
      val rtb = RptUtils.calculateRtb(log.iseffective, log.isbilling,log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
      val showClick = RptUtils.calculateShowClick(log.requestmode, log.iseffective)

      (newAppname,req++rtb++showClick)
    }).reduceByKey((list1, list2) => {
      // value进行计算
      // zip ((A1,B1),(A2,B2),..(A9,B9)) => ((A1+B1),...(A9+B9))
      list1.zip(list2).map(t => t._1 + t._2)
      // 省，市，指标1,指标2
    }).map(t => t._1+","+t._2.mkString(","))
      .saveAsTextFile(resultOutputPath)



    sc.stop()



}
