package com.flink.common.source

import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaMessge
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import scala.collection.JavaConverters._
object KafkaSourceManager {

  /**
    *
    * @param topic
    * @param broker
    * @param reset
    * @param groupid
    * @param setStartFromTimestamp
    * @return
    */
  def getKafkaSource(
      topic: String,
      broker: String,
      reset: String,
      groupid: String,
      setStartFromTimestamp: Long = 0L): FlinkKafkaConsumer010[KafkaMessge] = {
    val kafkasource = new FlinkKafkaConsumer010[KafkaMessge](
      topic.split(",").toList.asJava,
      new TopicMessageDeserialize(),
      KafkaManager.getKafkaParam(broker, groupid))
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    if (setStartFromTimestamp > 0) {
      kafkasource.setStartFromTimestamp(setStartFromTimestamp)
    } else {
      if (reset == "latest") {
        kafkasource.setStartFromLatest()
      } else if (reset == "earliest") {
        kafkasource.setStartFromEarliest()
      }
    }
    kafkasource
  }

}
