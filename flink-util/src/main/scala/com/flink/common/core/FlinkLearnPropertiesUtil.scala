package com.flink.common.core

import java.util.Date

import org.apache.commons.lang3.time.DateFormatUtils

/**
  *
  */
object FlinkLearnPropertiesUtil extends PropertiesTrait{
  lazy val TOPIC = getProperties("kafka.source.topics")
  lazy val BROKER = getProperties("kafka.broker")
  lazy val CHECKPOINT_PATH = getProperties("flink.checkpoint.path")+ s"/${DateFormatUtils.format(new Date(), "yyyyMMddHHMM")}"
  lazy val CHECKPOINT_INTERVAL = getProperties("flink.checkpoint.interval").toLong
  lazy val ES_HOSTS = getProperties("es.hosts")
  lazy val ES_CLUSTERNAME = getProperties("es.clustername")

  lazy val MYSQL_HOST = getProperties("mysql.host")
  lazy val MYSQL_USER = getProperties("mysql.user")
  lazy val MYSQL_PASSW = getProperties("mysql.passw")


  lazy val TEST_TOPIC = getProperties("kafka.topic.test")
  lazy val FLINK_DEMO_CHECKPOINT_PATH = getProperties("flink.checkpoint.path") + s"${proName}/${DateFormatUtils
    .format(new Date(), "yyyyMMddHHMM")}"


}
