package cn.dmp.tags

import org.apache.spark.sql.Row
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.catalyst.util.StringUtils

object Tags4Ads  extends Tags {
  override def markeTags(args: Any*): Map[String, Int] = {
    // 强转
    val row: Row = args(0).asInstanceOf[Row]

    var map = Map[String,Int]()


    // 广告类型和名称
    val adTypeId: Int = row.getAs[Int]("adspacetype")
    val adTypeName: String = row.getAs[String]("adspacetypename")

    if(adTypeId > 9) map += "LC" +adTypeId -> 1

    else if(adTypeId >0) map += "LC0" +adTypeId -> 1

    if (StringUtils.isNotEmpty(adTypeName)) map += "LN"+adTypeName -> 1

    //渠道的
    val chanelId = row.getAs[Int]("adplatformproviderid")
    if(chanelId > 0) map += (("CN"+chanelId,1))

    map
  }
}
