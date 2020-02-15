package cn.dmp.tags

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object testaaa {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSort2").setMaster("local[*]")
    val sc = new SparkContext(conf)

//    val a = Map[String,Int]("A" -> 1,"B" -> 2)
//    val b = Map[String,Int]("A" -> 3,"B" -> 2,"C" -> 4)

//    val sca = sc.parallelize(a)

    val x: RDD[(String, List[(String, Int)])] = sc.parallelize(List(
      ("a", Map[String, Int]("A" -> 1, "B" -> 2).toList),
      ("a", Map[String, Int]("A" -> 10, "D" -> 2).toList),
      ("b", Map[String, Int]("A" -> 10, "D" -> 2).toList)
    ))

  val r = x.reduceByKey((a,b) => {
    (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2)).toList})



    println(r.collect().toBuffer)


  }

}
