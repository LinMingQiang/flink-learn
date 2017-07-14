package com.flink.common.param

trait EnvironmentalKey {
  val KAFKA_BROKER = "kylin-node4:9092,kylin-node3:9092,kylin-node2:9092"
  val HBASE_ZOOKEEPER = "kylin-node4,kylin-node3,kylin-node2"
  val KAFKA_ZOOKEEPER = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  val BROKER = "kafka1:9092,kafka2:9092,kafka3:9092"
  val TOPIC = "smartadsclicklog"
}