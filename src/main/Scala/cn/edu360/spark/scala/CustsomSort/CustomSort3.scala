package cn.edu360.spark.scala.CustsomSort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/***
 * 使用Case class并且不传入名字作为参数. 可以不加with Serializable
 */
object CustomSort3 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("CustomSort2").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    //排序规则: 首先按照颜值,然后按照年龄升序
    val users: Array[String] = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 99","laoliu 28 100")

    //将River端的数据并行化成RDD
    val lines : RDD[String] = sc.parallelize(users)

    //切分整理数据
    val userRDD: RDD[(String,Int,Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      (name, age, fv)
    })

    // 将RDD里面装的数据进行排序 (传入了一个排序规则,不会改变数据的格式,只会改变顺序)
    val sorted: RDD[(String, Int, Int)] = userRDD.sortBy(tp =>  User3(tp._2,tp._3))


    println(sorted.collect().toBuffer)
    sc.stop()

  }

}

//Case class 自动实现Serializable和不需要写New. Ordered 必须指定比较类型
case class User3(age:Int, fv: Int) extends Ordered[User3] {
  override def compare(that: User3): Int = {
    if(this.fv == that.fv){
      //升序
      this.age - that.age
    }
    else{
      // 降序
      -(this.fv - that.fv)
    }
  }


}
