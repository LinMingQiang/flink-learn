package com.flink.common.param

trait EnvironmentalKey {
  val HBASE_ZOOKEEPER = ""
  val KAFKA_ZOOKEEPER = "ucloud-cdh-04,ucloud-cdh-05,ucloud-cdh-06"
  val BROKER = "local"
  val TOPIC = "topic"
}
