package com.flink.pro.func

import com.flink.pro.common.CaseClassUtil.{
  ReportInfo,
  ReportValues,
  SourceLogData
}
import com.flink.pro.common.StreamPropertiesUtil
import com.flink.pro.common.StreamPropertiesUtil._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

object DistinctReqidDataFormatUtil extends Serializable {
  lazy val _log =
    LoggerFactory.getLogger(DistinctReqidDataFormatUtil.getClass)
  val PATTERN = "yyyy-MM-dd HH"

  /**
    * 将数据转化为报表的维度和
    * @param in
    * @param o
    */
  def transAndFormatLog(in: SourceLogData, o: Collector[ReportInfo]): Unit = {
    // scalastyle:off
    try {
      in.topic match {
        case REQUEST_TOPIC => o.collect(setRequestRpt(in))
        case IMPRESS_TOPIC =>
        case CLICK_TOPIC   =>
        case _             => _log.error("-- topic not match : " + in.topic)
      }
    } catch {
      case t: Throwable =>
        _log.error(in.data.mkString("|"));
        _log.error(t.toString)
    }
  }

  /**
    *
    * @param in
    */
  def setRequestRpt(in: SourceLogData): ReportInfo = {
    val username = in.data(2)
    val time = DateFormatUtils.format(in.data(0).toLong, PATTERN)
    val day = time.substring(0, 10)
    val hour = time.substring(11, 13)
    val groupKys = Array(day, hour, username)
    ReportInfo(
      tablename = StreamPropertiesUtil.MYSQL_REPORT_TBL,
      keybyKey = DigestUtils.md5Hex(groupKys.mkString(",")),
      groupKey = groupKys,
      rv = ReportValues(bidReqNum = 1)
    )
  }
}
