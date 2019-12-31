package ScalaLesson

object ArrayOperation {

  // 定义一个数组
  val array = Array [Int](2,4,6,9,3)
  // map 方法是将array 数组中的每个元素进行某种映射操作, (x: Int) => x * 2 为 一个匿名函数, x 就是array 中的每个元素
  val y = array map((x: Int) => x * 2)
  // 或者这样写, 编译器会自动推测x 的数据类型
  val z = array .map(x => x*2)
  // 亦或者, _ 表示入参, 表示数组中的每个元素值
  val x = array .map(_ * 2)


  // 定义一个数组
  val words = Array ("hello tom hello jim hello jerry", "hello Hatano")
  // 将数组中的每个元素进行分割
  // Array(Array(hello, tom, hello, jim, hello, jerry), Array(hello,Hatano))
  val splitWords : Array[Array[String]] = words.map(wd => wd.split(" "))
  // 此时数组中的每个元素进过split 之后变成了Array, flatten 是对splitWords里面的元素进行扁平化操作
  // Array(hello, tom, hello, jim, hello, jerry, hello, Hatano)
  val flattenWords = splitWords.flatten
  // 上述的2 步操作, 可以等价于flatMap, 意味先map 操作后进行flatten 操作
  val result : Array[String] = words.flatMap(wd => wd.split(" "))
  // 遍历数组, 打印每个元素
  result.foreach(println)

  def main(args: Array[String]): Unit = {

    // map 返回自定义元祖，groupBy返回KV对，V被Array包裹

    // WordsCount 教程

    // 分词后压平 (返回Array)
    words.flatMap(_.split(" "))
    //Array[String] = Array(hello, tom, hello, jim, hello, jerry, hello, Hatano)

    // GroupBy （返回Map）
    words.flatMap(_.split(" ")).groupBy(x => x)
    // res8: scala.collection.immutable.Map[String,Array[String]] = Map(tom -> Array(tom), jim -> Array(jim), Hatano -> Array(Hatano), hello -> Array(hello, hello, hello, hello), jerry -> Array(jerry))

    // map求出数组长度(返回Map)
    words.flatMap(w => w.split(" ")).groupBy(x => x).map(x => (x._1,x._2.length))
    //scala.collection.immutable.Map[String,Int] = Map(tom -> 1, jim -> 1, Hatano -> 1, hello -> 4, jerry -> 1)

    // mapValues求出数组长度(返回Map)
    words.flatMap(w => w.split(" ")).groupBy(x => x).mapValues(_.length)
    // scala.collection.immutable.Map[String,Int] = Map(tom -> 1, jim -> 1, Hatano -> 1, hello -> 4, jerry -> 1)

    //转换为List
    words.flatMap(w => w.split(" ")).groupBy(x => x).mapValues(_.length).toList

    // 排序
    words.flatMap(w => w.split(" ")).groupBy(x => x).mapValues(_.length).toList.sortBy(x=> x._2)
  }

}
