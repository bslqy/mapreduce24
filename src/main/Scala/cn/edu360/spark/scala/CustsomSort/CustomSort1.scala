package cn.edu360.spark.scala.CustsomSort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort1 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSort1").setMaster("local[4]")

    val sc: SparkContext = new SparkContext(conf)

    //排序规则: 首先按照颜值,然后按照年龄升序
    val users: Array[String] = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 99","laoliu 28 100")

    //将River端的数据并行化成RDD
    val lines : RDD[String] = sc.parallelize(users)

    //切分整理数据
    val userRDD: RDD[User1] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt
      new User1(name, age, fv)
    })
    // 将RDD里面装的User类型的数据进行排序
    val sorted: RDD[User1] = userRDD.sortBy(u => u)
    val r: Array[User1] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }

}

//必须要可序列化. Ordered 必须指定比较类型
class User1(val name:String, val age:Int, val fv: Int) extends Ordered[User1]  with Serializable {
  override def compare(that: User1): Int = {
    if(this.fv == that.fv){
      //升序
      this.age - that.age
    }
    else{
      // 降序
      -(this.fv - that.fv)
    }
  }

    // 重写toString()
  override def toString: String = s"name: $name,age: $age, fv:$fv"
}
