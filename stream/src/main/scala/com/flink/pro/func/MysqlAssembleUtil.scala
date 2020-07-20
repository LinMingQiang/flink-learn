package com.flink.pro.func

import java.sql.PreparedStatement

import com.flink.pro.common.CaseClassUtil.ReportInfo

object MysqlAssembleUtil {

  def assembleRptMysqlPSt(): (ReportInfo, PreparedStatement) => Unit = {
    (info: ReportInfo, prest: PreparedStatement) =>
      {
        val Array(day, hour, username) = info.groupKey
        prest.setString(1, info.keybyKey)
        prest.setString(2, day)
        prest.setString(3, hour)
        prest.setString(4, username)
        prest.setLong(5, info.rv.bidReqNum)
        prest.addBatch()
      }

  }
}
