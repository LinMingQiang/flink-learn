package com.flink.common.kafka

import java.util.Properties

import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, KafkaDeserializationSchema}

import scala.collection.JavaConversions._

object KafkaManager {
  /**
   *
   */
  def getKafkaParam(broker: String, groupID : String = "test") = {
    val pro = new Properties()
    pro.put("bootstrap.servers", broker)
    // pro.put("zookeeper.connect", KAFKA_ZOOKEEPER)
    pro.put("group.id", groupID)
    pro.put("auto.commit.enable", "false") //kafka 0.8-
    pro.put("enable.auto.commit", "false") //kafka 0.9+
    // pro.put("auto.commit.interval.ms", "60000")
    pro
  }

  case class KafkaMessge(topic: String, msg: String)
  case class KafkaTopicOffsetMsg(topic: String, offset: Long, msg: String)

  /**
   *
   * @param topic
   * @param broker
   * @return
   */
  def getKafkaSource[T](topic: String,
                        broker: String,
                        deserialize: KafkaDeserializationSchema[T]) = {
    new FlinkKafkaConsumer010[T](topic.split(",").toList,
      deserialize,
      getKafkaParam(broker))
  }
}
