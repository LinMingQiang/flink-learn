package com.ddlsql

object DDLQueryOrSinkSQLManager {

  /**
   * 以1分钟为窗口，如果定了 watermark，数据展示时间 = 2+wartermark才会打印出来
   * 滚动窗口
   * https://help.aliyun.com/document_detail/62511.html?spm=a2c4g.11186623.6.664.4a7834edRreGGZ
   * @param tbl
   */
  def tumbleWindowSink(tbl: String): String = {
    s"""SELECT
       |TUMBLE_START(ts, INTERVAL '1' MINUTE),
       |TUMBLE_END(ts, INTERVAL '1' MINUTE),
       |username,
       |COUNT(url)
       |FROM $tbl
       |GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE),username""".stripMargin
  }



  def rowSink(): Unit = {
    s"""inc ROW<newAuthUserCount VARCHAR>"""
  }

}

