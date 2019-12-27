package com.flink.common.bean

object CaseClassUtil {
  case class ReportInfo(var indexName: String = "",
                        var keybyKey: String = "",
                        var groupKey: Array[String] = null,
                        var rv: ReportValues = null) {
    override def toString: String = {
      s"$indexName,$keybyKey : $rv"
    }
  }

  case class ReportValues(var income: Long = 0,
                          var clickNum: Long = 0,
                          var impressionNum: Long = 0,
                          var bidReqNum: Long = 0,
                          var fillNum: Long = 0){
    override def toString: String = {
      s"""req->${bidReqNum},fill->$fillNum,im->${impressionNum},click->${clickNum}"""
    }
  }


  case class Wordcount(w: java.lang.String, var c: java.lang.Long)

}
