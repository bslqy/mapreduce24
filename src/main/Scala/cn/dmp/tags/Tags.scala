package cn.dmp.tags

/**
 * 打标签的方法定义
 */
trait Tags {
  def markeTags(args: Any*):Map[String,Int]

}
