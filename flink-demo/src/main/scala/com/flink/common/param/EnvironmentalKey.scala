package com.flink.common.param

trait EnvironmentalKey {
  val HBASE_ZOOKEEPER = ""
  val KAFKA_ZOOKEEPER = "ucloud-cdh-04,ucloud-cdh-05,ucloud-cdh-06"
  val BROKER = "10.6.161.208:9092,10.6.161.209:9092,10.6.161.210:9092,10.6.161.211:9092,10.6.161.212:9092"
  val TOPIC = "mobssprequestlog_p15"
}
