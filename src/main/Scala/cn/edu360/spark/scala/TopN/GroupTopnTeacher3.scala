package cn.edu360.spark.scala.TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable


/***
 * 自定义分区后一个分区只有一个学科
 */

object GroupTopnTeacher3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupTopnTeacher3").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val topN = args(1).toInt


    val lines:RDD[String] = sc.textFile(args(0))
    // http://bigdata.edu360.cn/laozhang

    // ((subject,teacher),1)
    val subjectAndTeacher: RDD[((String,String),Int)] = lines.map(line => {
      ((line.split("/")(2).split("[.]")(0),
      line.split("/")(3)
      ),1)}
    )

    // 将学生和老师联合一起当做Key  ((subject,teacher),n)
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    // 计算有多少个学科 (用subject)
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    val sbParitioner = new SubjectParitioner(subjects);

    // 自定义一个分区器，并且按照指定的分区器进行分区. 提前计算有多少个学科
    // 调用partionBy时RDD的Key是一个tuple
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbParitioner)

    // mapPartitions()方法可以操作一个分区，即得到一个迭代器
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(
      // 讲迭代器转换成list，然后排序，再转换成迭代器返回
      it => it.toList.sortBy(_._2).take(topN).iterator
    )

    val r: Array[((String, String), Int)] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }


}

class SubjectParitioner(subs:Array[String]) extends Partitioner{

  //用于存放规则的一个map。一个学科进去就会有一个编号
  //相当于主构造器
  val rules = new mutable.HashMap[String,Int]()
  var i = 0
  for (s <- subs){
    rules.put(s,i)
    i += 1

  }


  //返回分区的数量 (下一个RDD有多少分区)
  override def numPartitions: Int = subs.length

  //根据传入的key计算分区标号
  //key是一个元组 （String，String). 实现分区规则
  override def getPartition(key: Any): Int = {
    val subject = key.asInstanceOf[(String,String)]._1
    // rules.get
    rules(subject)
  }
}

