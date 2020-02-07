package cn.dmp.report

import cn.dmp.bean.Log
import cn.dmp.utils.{JedisPool, RptUtils}
import org.apache.spark.{SparkConf, SparkContext}

object AppAnalysisRedis {
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
    val Array(inputPath, resultOutputPath) = args

    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    // 读取App字典
    sc.textFile(inputPath)
        .map(_.split(",",-1))
        .filter(_.length >=85)
        .map(Log(_)).filter(log => !log.appid.isEmpty || !log.appname.isEmpty)
        .mapPartitions(itr => {  // mapPartitions 必须返回迭代器。
          val jedis = JedisPool.getJedis()
          // jedis需要关闭，但是用mapPartition的话要返回迭代器，所以要新建ListBuffer存储结果
          val parResult = new collection.mutable.ListBuffer[(String,List[Double])]()

          //查询Redis
          itr.foreach(log => {
            var newAppname = log.appname
            if ("".equals(newAppname)) {
              newAppname = jedis.get(log.appid)
            }
            val req = RptUtils.caculateReq(log.requestmode, log.processnode)
            val rtb = RptUtils.caculateRtb(log.iseffective, log.isbilling, log.isbid, log.adorderid, log.iswin, log.winprice, log.adpayment)
            val showClick = RptUtils.caculateShowClick(log.requestmode, log.iseffective)
            parResult += ((newAppname,req++rtb++showClick))

          })
          jedis.close()
          parResult.toIterator
        }) // 返回迭代器

    sc.stop()
  }

}
