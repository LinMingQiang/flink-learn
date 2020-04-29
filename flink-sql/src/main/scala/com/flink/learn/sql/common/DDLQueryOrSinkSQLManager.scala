package com.flink.learn.sql.common

object DDLQueryOrSinkSQLManager {

  /**
   * 以1分钟为窗口，如果定了 watermark，数据展示时间 = 2+wartermark才会打印出来
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

}

