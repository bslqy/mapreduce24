package cn.dmp.tools

import ch.hsr.geohash.GeoHash
import cn.dmp.utils.{BaiduGeoAPI, JedisPool}
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

/***
 * 用来抽取日志字段中的经纬度，并且请求百度api获得商圈信息
 */
object ExtractLatLong2Business {
  def main(args: Array[String]): Unit = {

    // 0 校验参数个数
    if (args.length != 1) {
      println(
        """
          |cn.dmp.tags.ExtractLatLong2Business
          |参数.
          |输入路径
          |""".stripMargin)
      sys.exit()
    }


    val Array(inputPath) = args

    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //val sc = new SparkContext(sparkConf)
    val session = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = session.read.parquet(inputPath)

        df.select("long","lat")
          .where("lat > 3 and lat < 54 and long > 73 and long< 136").distinct()
          .rdd.foreachPartition(itr => {

          val jedis: Jedis = JedisPool.getJedis()

          itr.foreach( row => {

            val lat = row.getAs[String]("lat")
            val long = row.getAs[String]("long")

            val geoHashCode = GeoHash.withCharacterPrecision(lat.toDouble,long.toDouble, 8).toBase32
            val business = BaiduGeoAPI.getBUsiness(lat + ","+long)

            if (StringUtils.isNotEmpty(business)){
              //存入Redis
              jedis.set(geoHashCode,business)
            }


          })
          jedis.close()


          })


  }
}
