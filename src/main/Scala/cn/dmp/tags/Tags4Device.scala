package cn.dmp.tags

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Row

object Tags4Device extends Tags {
  override def markeTags(args: Any*): Map[String, Int] = {

    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]

    val os = row.getAs[Int]("client")
    val phoneType = row.getAs[String]("device")
    val ntm = row.getAs[String]("networkmannername")
    val ispname = row.getAs[String]("ispname")

    os match {
      case 1 => map += "D001001" -> 1
      case 2 => map += "D001002" -> 1
      case 3 => map += "D001003" -> 1
      case _ => map += "D001004" -> 1

    }


    if(StringUtils.isNotEmpty(phoneType)) map += "DN" + phoneType -> 1

    ntm.toUpperCase match {
      case "WIFI" => map += "D001001" -> 1
      case "4G" =>  map += "D002002" -> 1
      case "3G" =>  map += "D002003" -> 1
      case "2G" =>  map += "D002004" -> 1
      case _ =>  map += "D002005" -> 1
    }
    ispname match{
      case "移动" => map += "D003001" -> 1
      case "联通" => map += "D003002" -> 1
      case "电信" => map += "D003003" -> 1
      case _ => map += "D003004" -> 1
    }

    val pName = row.getAs[String]("provincename")
    val cName = row.getAs[String]("cityname")

    if(StringUtils.isNotEmpty(pName)) map += "ZP" +pName -> 1
    if(StringUtils.isNotEmpty(cName)) map += "ZP" +cName -> 1


    map

  }
}
