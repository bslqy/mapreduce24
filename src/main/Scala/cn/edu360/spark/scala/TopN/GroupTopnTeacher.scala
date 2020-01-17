package cn.edu360.spark.scala.TopN

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupTopnTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TopnTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val lines:RDD[String] = sc.textFile("D:\\UoM\\mapreduce24\\SparkTest\\teacher")
    // http://bigdata.edu360.cn/laozhang

    // ((subject,teacher),1)
    val subjectAndTeacher: RDD[((String,String),Int)] = lines.map(line => {
      ((line.split("/")(2).split("[.]")(0),
      line.split("/")(3)
      ),1)}
    )

    // 将学生和老师联合一起当做Key  ((subject,teacher),n)
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    // 分组排序 (subject,((subject,teacher),n))
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    // 经过分组后，一个分区内可能有很多学科的数据，一个学科就是一个迭代器
    // grouped.mapValues(_.toList.sortBy(_._2))  -- > List((subject,teacher),n)) 取n作为排序根据
    // 为什么可以调用scala的sortBy方法？ 因为一个学科的数据已经在一台机器上的一个集合里了
    val sorted: RDD[(String, List[((String, String), Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(3))

    val r: Array[(String, List[((String, String), Int)])] = sorted.collect()

    // 分组排序(按学科进行分组)
    println(r.toBuffer)




    sc.stop()




  }

}
