package com.flink.learn.bean

import org.apache.flink.api.common.typeinfo.TypeInformation

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
                          var fillNum: Long = 0) {
    override def toString: String = {
      s"""req->${bidReqNum},fill->$fillNum,im->${impressionNum},click->${clickNum}"""
    }
  }

  // 这样是不行的，This type (GenericType<com.flink.learn.bean.CaseClassUtil.TransWordCount>) cannot be used as key.
  // 需要有一个无参的构造函数
  // case class TransWordCount(var word: String = "", var count: Long = 0L, var lastTime: String = "")
  // 这样是可以的
  case class TransWordCount(var word: String,
                            var count: Long,
                            var timestamp: Long) {
    override def toString: String =
      s"""TransWordCount($word,$count,$timestamp)"""
  }

  case class Wordcount(word: String, var count: Long, timestamp: Long)  extends Serializable

  case class SessionLogInfo(sessionId: String, timeStamp: Long)

  /**
    *
    * @param session
    * @param count 次数
    * @param internalTime 间隔时长
    */
  case class SessionWindowResult(session: String,
                                 count: Int,
                                 internalTime: Long)
}
