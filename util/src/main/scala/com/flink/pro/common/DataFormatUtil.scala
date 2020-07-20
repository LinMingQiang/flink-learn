package com.flink.pro.common

object DataFormatUtil {
  def gaCodeAnalysis(gacode: String): (String, String, String) = {
    if (gacode.isEmpty || gacode.size < 10 || gacode == "1000000000" || gacode
      .substring(0, 4) != "1156") {
      // ("9999", "99", "99")
      ("-1", "-1", "-1")
    } else {
      (gacode.substring(0, 4), gacode.substring(4, 6), gacode.substring(6, 8))
    }
  }

  /**
   * 格式化数据，赋值默认值-1，对接 java报表模块
   * @param str
   * @return
   */
  def formatDimension(str: String): String = {
    if (str.isEmpty) "-1" else str
  }
}
