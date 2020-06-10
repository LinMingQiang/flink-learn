package com.flink.learn.entry

import com.flink.common.core.{
  EnvironmentalKey,
  FlinkEvnBuilder,
  FlinkLearnPropertiesUtil
}
import org.apache.flink.streaming.api.scala._

import com.flink.common.core.FlinkLearnPropertiesUtil.{
  BROKER,
  FLINK_DEMO_CHECKPOINT_PATH,
  TEST_TOPIC,
  param
}
import com.flink.common.deserialize.TopicOffsetMsgDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg
import com.flink.learn.richf.WordCountRichFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object WordCount {
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "LocalFlinkTest")
    //    kafkasource.setStartFromSpecificOffsets(
    //      Map(new KafkaTopicPartition("maxwell_new", 0) -> 1L.asInstanceOf[java.lang.Long]));
    val env = FlinkEvnBuilder.buildStreamingEnv(param,
                                                FLINK_DEMO_CHECKPOINT_PATH,
                                                60000) // 1 min
    env
      .addSource(kafkaSource(TEST_TOPIC, BROKER))
      .flatMap(_.msg.split("\\|", -1))
      .map(x => (x, 1))
      .keyBy(0)
      .flatMap(new WordCountRichFunction)
      .print
    env.execute("lmq-flink-demo") //程序名
  }

  /**
    *
    * @param topic
    * @param broker
    * @return
    */
  def kafkaSource(
      topic: String,
      broker: String): FlinkKafkaConsumer010[KafkaTopicOffsetMsg] = {
    val kafkasource = KafkaManager.getKafkaSource(
      topic,
      broker,
      new TopicOffsetMsgDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromEarliest() //不加这个默认是从上次消费
    kafkasource
  }
}
