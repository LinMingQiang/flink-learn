package com.flink.common.param

trait EnvironmentalKey {
  val HBASE_ZOOKEEPER = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  val KAFKA_ZOOKEEPER = "solr2.zhiziyun.com,solr1.zhiziyun.com,mongodb3"
  val BROKER = "kafka1:9092,kafka2:9092,kafka3:9092"
  val TOPIC = "smartadsdeliverylog,smartadsclicklog"
}