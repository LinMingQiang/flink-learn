package com.flink.common.core

import java.util.Date

import org.apache.commons.lang3.time.DateFormatUtils

/**
  *
  */
object FlinkLearnPropertiesUtil extends PropertiesTrait{
  def getProperties(key: String): String = param.get(key)
  lazy val KAFKA_BROKER = getProperties("kafka.broker")
  lazy val KAFKA_REQ_LOG_TOPIC = getProperties("")



  lazy val MYSQL_HOST = getProperties("mysql.host")
  lazy val MYSQL_USER = getProperties("mysql.user")
  lazy val MYSQL_PASSW = getProperties("mysql.passw")

}
