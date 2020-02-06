package cn.dmp.report

import cn.dmp.utils.JedisPool
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import redis.clients.jedis.Jedis

object App2DictRedis {
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
    val Array(appDictInputPath, resultOutputPath) = args

    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KyroSerializer")

    val sc = new SparkContext(sparkConf)

    sc.textFile(appDictInputPath).map(line =>{
      val field = line.split("\t", -1)
      (field(4),field(1))
    }).foreachPartition(itr => {
      val jedis: Jedis = JedisPool.getJedis()

      itr.foreach(t => {
        jedis.set(t._1,t._2)
      })
    })




  }
}
