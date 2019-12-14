package com.flink.common.param

trait EnvironmentalKey {
  val HBASE_ZOOKEEPER = ""
  val KAFKA_ZOOKEEPER = ""
  val BROKER = "localhost:9092"
  val TOPIC = "test"
}
