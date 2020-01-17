package cn.edu360.spark.scala.CustsomSort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/***
 * Tuple3 默认实现了Compare方法, 先比第一个,再比第二个. 无需生成类即可比较
 */
object CustomSort5 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSort2").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //排序规则: 首先按照颜值,然后按照年龄升序
    val users: Array[String] = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 99","laoliu 28 100")

    //将River端的数据并行化成RDD
    val lines : RDD[String] = sc.parallelize(users)

    //切分整理数据
    val tpRDD: RDD[(String,Int,Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      (name, age, fv)
    })

    // 利用元组比较规则,先比第一个,相等再比第二个. 负数代表降序
    val sorted = tpRDD.sortBy(tp => (-tp._3, tp._2))


    println(sorted.collect().toBuffer)
    sc.stop()

  }

}
