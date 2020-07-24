package com.flink.common.core

import org.apache.flink.api.java.utils.ParameterTool

import scala.collection.JavaConverters._

trait PropertiesTrait {
  var proName = ""
  var param: ParameterTool = ParameterTool.fromMap(Map("" -> "").asJava)
  def getProperties(key: String): String = param.get(key)
  def getProperties(key: String, defualt: String): String = param.get(key, defualt)

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
