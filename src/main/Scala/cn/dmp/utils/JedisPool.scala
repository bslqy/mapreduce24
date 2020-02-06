package cn.dmp.utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object JedisPool {
  val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)

  val pool: JedisPool = new JedisPool(config,"192.168.226.11",6379,10000)

  def getJedis() = {
    pool.getResource
  }


}
