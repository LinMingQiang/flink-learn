package com.flink.common.param

import java.util.Date

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.java.utils.ParameterTool

object PropertiesUtil {
  var param: ParameterTool = null
  def init(path: String): Unit = {
    println("> path : " + path)
    param = ParameterTool.fromPropertiesFile(path)
    println(param.getConfiguration)
  }
  def init(param: ParameterTool): Unit = {
    this.param = param
  }
  def getProperties(key: String): String = param.get(key)
  lazy val TOPIC = getProperties("kafka.source.topics")
  lazy val BROKER = getProperties("kafka.brokers")
  lazy val CHECKPOINT_PATH = getProperties("flink.checkpoint.path")+ s"/${DateFormatUtils.format(new Date(), "yyyyMMddHHMM")}"
  lazy val CHECKPOINT_INTERVAL = getProperties("flink.checkpoint.interval").toLong
  lazy val ES_HOSTS = getProperties("es.hosts")
  lazy val ES_CLUSTERNAME = getProperties("es.clustername")



  lazy val BUYER_INDEX = getProperties("es.buyer_index")
  lazy val SELLER_INDEX = getProperties("es.seller_index") // appid
  lazy val INDEX_TYPE = "sspreport_flink_type"



  lazy val IMPRESS_TOPIC = getProperties("kafka.impression.topic")
  lazy val CLICK_TOPIC = getProperties("kafka.click.topic")
  lazy val BIDDING_TOPIC = getProperties("kafka.bid.topic")
  lazy val REQUEST_TOPIC =getProperties( "kafka.request.topic")

  lazy val REQ_BID_LOG_SIZE = 38// getProperties("req_response_log_size").toInt
  lazy val IMPRESS_CLICK_LOG_SIZE = 25// getProperties("impres_click_log_size").toInt
}
