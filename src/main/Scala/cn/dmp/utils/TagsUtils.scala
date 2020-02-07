package cn.dmp.utils

object TagsUtils {
  val hasSomeUserIdCondition =  """
                                  |ime != "" or imeimd5 != "" or imeisha1 !=""
                                  |idfa != "" or idfamd5 != "" or idfasha1 !=""
                                  |mac != "" or macmd5 != "" or macsha1 !=""
                                  |andrioidid != "" or andrioididmd5 != "" or andrioididsha1 !=""
                                  |openudid != "" or openudidmd5 != "" or openudidsha1 !=""
                                """.stripMargin

}
