package com.flink.common.core

import java.io.{File, InputStreamReader}
import org.apache.flink.api.java.utils.ParameterTool

import java.util
import java.util.Properties
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
    val file = new File(path)
    if (file.exists()) {
      param = ParameterTool.fromPropertiesFile(path)
    } else {
      if (path.endsWith("properties")) {
        val in = this.getClass.getClassLoader.getResourceAsStream("application.properties")
        val props = new Properties
        val inputStreamReader = new InputStreamReader(in, "UTF-8")
        props.load(inputStreamReader)
        param = ParameterTool.fromMap(props.asScala.toMap.asJava)
      }
    }
    param.toMap.asScala.foreach(println)
  }

  /**
   * 用于driver端初始化
   */
  def init(proName: String, path: java.util.List[String]): Unit = {
    this.proName = proName
    var map = new util.HashMap[String, String]()
    path.asScala.foreach(p => {
      val file = new File(p)
      if (file.exists()) {
        map.putAll(ParameterTool.fromPropertiesFile(p).toMap)
      }
    })
    param = ParameterTool.fromMap(map)
  }

  /**
   * 用于在operate func初始化
   *
   * @param param
   */
  def init(param: ParameterTool): Unit = {
    this.param = param
  }

}
