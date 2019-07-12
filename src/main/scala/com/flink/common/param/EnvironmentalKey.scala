package com.flink.common.param

trait EnvironmentalKey {
  val HBASE_ZOOKEEPER = ""
  val KAFKA_ZOOKEEPER = "ucloud-cdh-04,ucloud-cdh-05,ucloud-cdh-06"
  val BROKER = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
  val TOPIC = "smartadsdeliverylog"
}
