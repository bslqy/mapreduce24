package cn.dmp.graphx


import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object commonFriend {

  def main(args: Array[String]): Unit = {
    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)

    //点
    val uv: RDD[(VertexId, (String, Int))] = sc.parallelize(Seq(
      (1, ("和川", 23)),
      (2, ("林汉伦", 23)),
      (6, ("长亭", 23)),
      (9, ("杨洋", 23)),
      (133, ("杨洋", 23)),

      (16, ("杨俊", 23)),
      (21, ("刘瑞川", 25)),
      (44, ("崔斌", 20)),
      (138, ("张贤", 31)),

      (5, ("高景涛", 25)),
      (7, ("陈芳", 19)),
      (158, ("张先义", 26))))

    //边
    val ue: RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(9, 133, 0),
      Edge(6, 133, 0),

      Edge(6, 138, 0),
      Edge(16, 138, 0),
      Edge(44, 138, 0),
      Edge(21, 138, 0),

      Edge(5, 158, 0),
      Edge(7, 158, 0)

    ))
    ue

    // 调用API就会让所有边向最小的顶点对齐
    val graph: Graph[(String, Int), Int] = Graph(uv,ue)
    // 连通图
    //    (16,1)
    //    (44,1)
    //    (21,1)
    //    (133,1)
    //    (1,1)
    //    (9,1)
    //    (5,5)
    //    (158,5)
    //    (138,1)
    //    (6,1)
    //    (2,1)
    val commonV: VertexRDD[VertexId] = graph.connectedComponents().vertices
    // 交换位置,然后按最小ID的用户聚合,则用户A拥有一个好友的列表
    // commonV.map(t => (t._2,List(t._1))).reduceByKey(_++_)
    /**
     * uv --> (userId, (姓名,年龄))
     * commonV --> (userId, 共同的顶点))
     */
    uv.join(commonV).map{
      case (userId, ((name,age), cmId)) =>  (cmId, List((name,age)))
    }.reduceByKey(_ ++ _)

  }
}
