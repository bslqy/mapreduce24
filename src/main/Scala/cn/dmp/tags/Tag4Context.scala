package cn.dmp.tags


import cn.dmp.utils.{JedisPool, TagsUtils}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{SQLContext, SparkSession}

import scala.collection.mutable.ListBuffer

object Tag4Context {
  def main(args: Array[String]): Unit = {
    // 0 校验参数个数
    if (args.length != 5) {
      println(
        """
          |cn.dmp.report.Tag4Context
          |参数.
          |输入路径
          |字典路径
          |停用词库路径
          |日期
          |输出路径
          |""".stripMargin)
      sys.exit()
    }

    // 1 接受程序的参数
    val Array(inputPath, dictFilePath, stopWordsFilePath,day, outputPath) = args

    //2 创建SparkContext
    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"${this.getClass.getSimpleName}")
    sparkConf.setMaster("local[*]")
    // RDD 序列化到磁盘 worker与worker之间的数据传输
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(sparkConf)
    val session = SparkSession.builder().config(sparkConf).getOrCreate()

    //3 Collect为了收集切片，要不然可能只是部分
    val dictMap: Map[String, String] = sc.textFile(dictFilePath).map(line => {
      val field = line.split("\t", -1)
      (field(4), field(1))
    }).collect().toMap

    // 停用词字典文件
    val stopWordMap: Map[String, Int] = sc.textFile(dictFilePath).map((_, 0)).collect().toMap


    // 4 广播字典文件到executor
    val broadcastAppDict: Broadcast[Map[String, String]] = sc.broadcast(dictMap)
    val broadcastStopWordsDict: Broadcast[Map[String, Int]] = sc.broadcast(stopWordMap)

    val load = ConfigFactory.load()
    val hbTableName = load.getString("hbase.table.name")
    //判断hbase中的表是否存在，如果不存在则创建
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.zookeeper.host"))
    val hbConn: Connection = ConnectionFactory.createConnection(configuration)
    val hbAdmin = hbConn.getAdmin

    if (!hbAdmin.tableExists(TableName.valueOf(hbTableName))){
      println(s"$hbTableName 不存在...")
      println(s"正在创建  $hbTableName ...")

      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbTableName))
      val columnDescriptor = new HColumnDescriptor(s"day$day")
      tableDescriptor.addFamily(columnDescriptor)
      hbAdmin.createTable(tableDescriptor)

      // 释放连接
      hbAdmin.close()
      hbConn.close()
    }
    val jobConf: JobConf = new JobConf(configuration)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    // 指定表的名称
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,hbTableName)

    // 读取日誌的parquet文件
    session.read.parquet(inputPath).where(TagsUtils.hasSomeUserIdCondition).rdd.mapPartitions(par => {
      // 把数据传入打标签的类，Tags4Ads 类里面实现打标签接口方法

      // 每个分区开启一个Jedis客户端
      val jedis = JedisPool.getJedis()
      val listBuffer = new ListBuffer[(String,List[(String,Int)])]()

      par.foreach(row => {
        val ads = Tags4Ads.markeTags(row)
        val apps = Tags4App.markeTags(row, broadcastAppDict.value)
        val devices = Tags4Device.markeTags(row)
        val keywords = Tag4KeyWords.markeTags(row, broadcastAppDict.value, broadcastStopWordsDict.value)
        val allUserId = TagsUtils.getAllUserId(row)

        //商圈的标签
        val business = Tags4Business.markeTags(row, jedis)

        listBuffer.append((allUserId(0), (ads ++ apps ++ devices ++ keywords ++ business).toList))
        listBuffer

      })
      jedis.close()

      listBuffer.iterator
    }).reduceByKey((a,b) => {
      // val a = ("ID1",List(("电视剧" -> 2))) val b = ("ID1",List(("体育" -> 1),("电视剧" -> 1)))
      //  => (a ++ b)

      //( "ID1",List(("电视剧" -> 2),("体育" -> 1),("电视剧" -> 1)) )
      // => groupBy(_._1)
      //  Map( "电视剧"-> List(("电视剧"-> 2),("电视剧"-> 1)) , "体育" -> List(("体育" -> 1)) )

      // => mapValues(_.foldLeft(0)(_+ _._2)).toList
      // RDD[ "ID1",List[("电视剧",3),("体育",1)] ]
      (a ++ b).groupBy(_._1).mapValues(_.foldLeft(0)(_+ _._2)).toList

    }).map{
      // 放进Hbase中
      case (userId,userTags) => {
        val put: Put = new Put((Bytes.toBytes(userId)))
        //把List转换成字符串
        val tags: String = userTags.map(t => t._1 + ":" + t._2).mkString(",")
        // 列族下的哪一个列
        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes(s"day$day"), Bytes.toBytes(tags))

        (new ImmutableBytesWritable(), put) //ImmutableBytesWritable => rowKey
      }
    }.saveAsHadoopDataset(jobConf)


    sc.stop()


  }

}
