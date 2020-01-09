package cn.edu360.spark.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupTopnTeacher2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupTopnTeacher2").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val topN = args(1).toInt

    val subjects = Array("bigdata","javaee","php")

    val lines:RDD[String] = sc.textFile("C:\\Users\\LiaoG\\HadoopTest\\Scala")
    // http://bigdata.edu360.cn/laozhang

    // ((subject,teacher),1)
    val subjectAndTeacher: RDD[((String,String),Int)] = lines.map(line => {
      ((line.split("/")(2).split("[.]")(0),
      line.split("/")(3)
      ),1)}
    )

    // 将学生和老师联合一起当做Key  ((subject,teacher),n)
    val reduced: RDD[((String, String), Int)] = subjectAndTeacher.reduceByKey(_+_)

    //scala集合排序是在内存中进行，可能不够用.RDD 的sortby方法进行内存+磁盘排序
    // 改RDD中对应的数据仅有一个学科的数据
    for (s <- subjects){
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == s)

      //现在调用的是RDD的sort方法, (take是一个action，会触发任务提交)
      val favTeacher = filtered.sortBy(_._2,false).take(topN)

      //打印
      println(favTeacher.toBuffer)

    }





    sc.stop()




  }

}
