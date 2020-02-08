package cn.dmp.tags

import org.apache.spark.sql.Row

object Tag4KeyWords extends Tags {

  override def markeTags(args: Any*): Map[String, Int] = {
    var map = Map[String,Int]()
    val row = args(0).asInstanceOf[Row]
    val stopWords = args(1).asInstanceOf[Map[String,Int]]

    val kws = row.getAs[String]("keywords")


    kws.split("\\|")
      .filter(kw => kw.length >= 3 && kw.length <= 8 && !stopWords.contains(kw))
        .foreach(kw => map += "K"+kw -> 1 )

    map

  }



}
