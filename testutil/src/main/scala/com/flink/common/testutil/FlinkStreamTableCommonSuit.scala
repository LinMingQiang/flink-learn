package com.flink.common.testutil

import com.flink.common.core.FlinkLearnPropertiesUtil.{FLINK_DEMO_CHECKPOINT_PATH, param}
import com.flink.common.core.{FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.deserialize.TopicOffsetMsgDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class FlinkStreamTableCommonSuit extends FunSuite with BeforeAndAfterAll {
  var tableEnv: StreamTableEnvironment = null

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    FlinkLearnPropertiesUtil.init("/Users/eminem/workspace/flink/flink-pro-demo/dist/conf/application.properties",
                                  "FlinkStreamTableCommonSuit")
    tableEnv = FlinkEvnBuilder.buildStreamTableEnv(param,
      FLINK_DEMO_CHECKPOINT_PATH,
                                                   10000,
                                                   Time.minutes(1),
                                                   Time.minutes(6))
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
