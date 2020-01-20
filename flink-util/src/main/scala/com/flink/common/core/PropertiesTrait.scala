package com.flink.common.core

import org.apache.flink.api.java.utils.ParameterTool

trait PropertiesTrait {
  var proName = ""
  var param: ParameterTool = null
  /**
   * 用于driver端初始化
   */
  def init(path: String, proName: String): Unit = {
    this.proName = proName
    param = ParameterTool.fromPropertiesFile(path)
  }

  /**
   * 用于在operate func初始化
   * @param param
   */
  def init(param: ParameterTool): Unit = {
    this.param = param
  }

}
