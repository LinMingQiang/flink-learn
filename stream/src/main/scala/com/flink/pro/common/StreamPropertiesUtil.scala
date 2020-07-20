package com.flink.pro.common

import java.util.Date

import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}
import org.apache.flink.api.java.utils.ParameterTool

object StreamPropertiesUtil extends PropertiesTrait {
  // kafka相关配置
  lazy val TOPIC = getProperties("kafka.source.topics")
  lazy val BROKER = getProperties("kafka.brokers")
  lazy val IMPRESS_TOPIC = getProperties("kafka.impression.topic")
  lazy val CLICK_TOPIC = getProperties("kafka.click.topic")
  lazy val REQUEST_TOPIC = getProperties("kafka.request.topic")
  // flink
  lazy val CHECKPOINT_PATH = getProperties("flink.checkpoint.path") + s"/stream/${DateFormatUtils
    .format(new Date(), "yyyyMMdd")}"
  lazy val CHECKPOINT_INTERVAL = getProperties("flink.checkpoint.interval").toLong

  // es相关配置
  lazy val ES_HOSTS = getProperties("es.hosts")
  lazy val ES_CLUSTERNAME = getProperties("es.clustername")
  lazy val SELLER_INDEX = getProperties("es.rpt.index")
  lazy val INDEX_TYPE = "flink_rpt_type"

  lazy val REQ_BID_LOG_SIZE = getProperties("req_response_log_size").toInt
  lazy val IMPRESS_CLICK_LOG_SIZE = getProperties("impres_click_log_size").toInt

  lazy val JDBC = getProperties("mysql.jdbc")
  lazy val USER = getProperties("mysql.user")
  lazy val PASSWD = getProperties("mysql.passw")
  lazy val MYSQL_REPORT_TBL = getProperties("mysql.tablename")
  def SDK_REPLACE_INSERT_SQL(tbl: String): String = {
    s"""replace into
       | `${tbl}`(md5key,
       | day,
       | hour,
       | username,
       | reqnum
       | )
       |  values(?,?,?,?,?)""".stripMargin
  }
}
