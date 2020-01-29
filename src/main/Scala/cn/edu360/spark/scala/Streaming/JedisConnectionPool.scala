package cn.edu360.spark.scala.Streaming

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {
  val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间(10秒)
  val pool = new JedisPool(config,"192.168.226.11",6379,10000)

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnection()
//
//    val r1 = conn.get("hyy")
//
//    println(r1)
//
//    conn.incrBy("hyy",20)
//
//    val r2 = conn.get("hyy")
//
//    println(r2)
//
//    conn.close()

    //取出所有的key，循环遍历
    val r = conn.keys("*")
    //需要导入隐式转换
    import scala.collection.JavaConversions._
    for(p<- r){
      println(p + " : " + conn.get(p))
    }
  }

}
