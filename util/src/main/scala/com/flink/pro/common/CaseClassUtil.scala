package com.flink.pro.common
object CaseClassUtil {

  /**
    * 报表维度
    * @param tablename
    * @param keybyKey
    * @param groupKey
    * @param rv
    */
  case class ReportInfo(var tablename: String = "",
                        var keybyKey: String = "",
                        var groupKey: Array[String] = null,
                        var rv: ReportValues = null) {
    override def toString: String = {
      s"$tablename,$keybyKey,${groupKey.mkString("|")} : $rv"
    }
  }

  /**
    * 原始日志过滤，reqid用于去重
    * @param topic
    * @param req_id
    * @param data
    */
  case class SourceLogData(topic: String, req_id: String, data: Array[String])

  /**
    * 报表指标
    * @param income
    * @param clickNum
    * @param impressionNum
    * @param bidReqNum
    * @param fillNum
    */
  case class ReportValues(var income: Long = 0,
                          var clickNum: Long = 0,
                          var impressionNum: Long = 0,
                          var bidReqNum: Long = 0,
                          var fillNum: Long = 0) {
    override def toString: String = {
      s"""req->${bidReqNum},fill->$fillNum,im->${impressionNum},click->${clickNum}"""
    }
  }

}
