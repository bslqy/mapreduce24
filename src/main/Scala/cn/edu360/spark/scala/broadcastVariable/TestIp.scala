package cn.edu360.spark.scala.broadcastVariable

import scala.io.{BufferedSource, Source}


/***
 *
 * 单机版本
 */
object TestIp {

  def ip2Long(ip:String) : Long = {
    val fragment = ip.split("[.]")
    var ipNum = 0L
    for (i<- 0 until fragment.length){
      ipNum = fragment(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines:Array[(Long, Long, String)],ip:Long): Int = {
    var low = 0
    var high = lines.length - 1

    while( low <= high){
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) &&  (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else{
        low = middle + 1
      }
    }
    -1
  }

  def readRules(path:String): Array[(Long,Long,String)] = {
    val bf: BufferedSource = Source.fromFile(path)

    val lines: Iterator[String] = bf.getLines()
    // 1.0.1.0|1.0.3.255|16777472|16778239|亚洲|中国|福建|福州||电信|350100|China|CN|119.306239|26.075302

    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fields: Array[String] = line.split("[|]")
      val start = fields(2).toLong
      val end = fields(3).toLong
      val province = fields(6)

      (start, end, province)

    }).toArray

    rules

  }

  def main(args: Array[String]): Unit = {

    // 数据是在内存中
    val rules: Array[(Long, Long, String)] = readRules("C:\\Users\\LiaoG\\HadoopTest\\Scala\\ip.txt")

    // 将ip地址转换成是十进制
    val ipNum = ip2Long("111.198.38.185")

    // 进行查找
    val index:Int = binarySearch(rules,ipNum)

    val tp: (Long, Long, String) = rules(index)

    val province = tp._3


    println(province)
  }

}
