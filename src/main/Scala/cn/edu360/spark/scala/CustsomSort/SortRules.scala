package cn.edu360.spark.scala.CustsomSort

object SortRules {
  implicit object OrderingUser extends Ordering[User4]{
    override def compare(x: User4, y: User4): Int = {
      if (x.fv == y.fv){
        x.age - y.age
      }
      else{
        y.fv - x.fv
      }

    }

  }

}
