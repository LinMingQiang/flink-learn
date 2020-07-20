package com.flink

import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaMessge
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import scala.collection.JavaConverters._

package object test {
  /**
   *
   * @param topic
   * @param broker
   * @return FlinkKafkaConsumer010
   */
  def getKafkaSource(
                      topic: String,
                      broker: String,
                      groupid: String,
                      setStartFromTimestamp: Long = 0L): FlinkKafkaConsumer010[KafkaMessge] = {
    val kafkasource = new FlinkKafkaConsumer010[KafkaMessge](
      topic.split(",").toList.asJava,
      new TopicMessageDeserialize(),
      KafkaManager.getKafkaParam(broker, groupid))
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    if(setStartFromTimestamp > 0)
      kafkasource.setStartFromTimestamp(setStartFromTimestamp)
    // kafkasource.setStartFromEarliest()
    //    if (rest.equals("latest"))
    //      kafkasource.setStartFromLatest() // 不加这个默认是从上次消费
    //    else if (rest.equals("earliest"))
    //      kafkasource.setStartFromEarliest()
    kafkasource
  }
}
