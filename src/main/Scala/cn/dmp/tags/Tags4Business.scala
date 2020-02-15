package cn.dmp.tags

import ch.hsr.geohash.GeoHash
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object Tags4Business extends Tags {
  override def markeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]

    val lat = row.getAs[String]("lat")
    val longs = row.getAs[String]("long")

    // lat > 3 and lat < 54 and long > 73 and long< 136
    if (StringUtils.isNotEmpty(lat) && StringUtils.isNotEmpty(longs)){

      var lat2 = lat.toDouble
      var lon2 = longs.toDouble

      if(lat2 > 3 && lat2 < 54 && lon2 > 73 && lon2 < 136){
        // 从Redis里面读取相应的商圈
        val geoHashCode = GeoHash.withBitPrecision(lat2,lon2,8).toBase32
        val business = jedis.get(geoHashCode)
        if(StringUtils.isNotEmpty(business)){
          jedis.get(geoHashCode).split(",").foreach(bs => map += "BS" + bs -> 1)
        }


      }
    }
    map


  }
}
