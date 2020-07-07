package com.flink.learn.test.common

import com.flink.common.core.FlinkLearnPropertiesUtil.{FLINK_DEMO_CHECKPOINT_PATH, param}
import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.deserialize.TopicOffsetMsgDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class FlinkStreamCommonSuit extends FunSuite with BeforeAndAfterAll {
  var env: StreamExecutionEnvironment = null

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "LocalFlinkTest")
    //    kafkasource.setStartFromSpecificOffsets(
    //      Map(new KafkaTopicPartition("maxwell_new", 0) -> 1L.asInstanceOf[java.lang.Long]));
    env = FlinkEvnBuilder.buildStreamingEnv(param,
                                            FLINK_DEMO_CHECKPOINT_PATH,
                                            60000) // 1 min
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
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
