package cn.edu360.spark.scala.Streaming

import cn.edu360.spark.scala.broadcastVariable.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object CalculateUtil {
  // ip 类目 品名 价格
  def calculateIncome(fields:RDD[Array[String]])={
    //将数据计算后写入Redis
    val priceRDD: RDD[Double] = fields.map(arr => {
      val price: Double = arr(4).toDouble
      price
    })

    // reduce是一个Action，会把结果返回到Drive端
    val sum: Double = priceRDD.reduce(_+_)
    //获取一个jedis的链接
    val conn: Jedis = JedisConnectionPool.getConnection()
    conn.incrByFloat("Total_Income",sum)
    //释放连接
    conn.close()

  }

  /**
   * 计算分类的成交金额
   * @param fields
   */
  def calculateItem(fields:RDD[Array[String]]) = {
    val itemAndPrice = fields.map(arr => {
      //分类
      val item = arr(2)
      //金额
      val price = arr(4).toDouble
      (item,price)
    })
    //按照商品分类进行聚合
    val reduced: RDD[(String, Double)] = itemAndPrice.reduceByKey(_+_)
    //将当前批次写入到Redis中 （可能有几千万种分类)
    //foreachPartition是一个Action

    //现在这种方式，jedis的连接是在（Driver）段创建的,并未实现序列化，无法把链接实例发送
    // val conn: Jedis = JedisConnectionPool.getConnection()
    reduced.foreachPartition(part => {
      // 这个链接其实是在Executor中获取的(此处在操作RDD，所以是Executor)
      // JedisConnectionPool在一个Executor中是单例的
      val conn: Jedis = JedisConnectionPool.getConnection()
      part.foreach(t =>{
        conn.incrByFloat(t._1,t._2)
      })
      //将当前分区中的数据更新完再关闭链接
      conn.close()
    })
  }


  //根据IP计算归属地
  def calcuateZone(fields:RDD[Array[(String)]], broadcastRef:Broadcast[Array[(Long, Long, String)]]): Unit ={
    val provinceAndPriceRDD: RDD[(String, Double)] = fields.map(arr => {
      val ip = arr(1)
      val price = arr(4).toDouble
      val ipNum = MyUtils.ip2Long(ip)
      //在Executor中获取广播变量的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value
      //二分法查找
      var province = "未知"
      val index: Int = MyUtils.binarySearch(allRules, ipNum)
      if (index != 1)
      {
        province = allRules(index)._3
      }
      //（省份，订单金额)
      (province, price)
    })

    //按省份聚合
    val reduced: RDD[(String, Double)] = provinceAndPriceRDD.reduceByKey(_+_)
    //更新Redis
    reduced.foreachPartition(part =>{
      val conn = JedisConnectionPool.getConnection()
      part.foreach( t=>{
        conn.incrByFloat(t._1,t._2)
      })
      conn.close()
    })

  }


}
