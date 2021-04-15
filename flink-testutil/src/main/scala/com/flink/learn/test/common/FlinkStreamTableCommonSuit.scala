package com.flink.learn.test.common

import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.core.FlinkLearnPropertiesUtil.{param, CHECKPOINT_PATH}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
class FlinkStreamTableCommonSuit extends FunSuite with BeforeAndAfterAll {
  var tableEnv: StreamTableEnvironment = null
  var tableE: TableEnvironment = null
  var streamEnv: StreamExecutionEnvironment = null
  var bEnv: ExecutionEnvironment = null
  var batchEnv: BatchTableEnvironment = null

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "FlinkLearnStreamDDLSQLEntry")
    tableEnv = FlinkEvnBuilder.buildStreamTableEnv(param,
                                                   CHECKPOINT_PATH,
                                                   10000,
                                                   Time.minutes(1),
                                                   Time.minutes(6))
    System.out.println("! INITIALIZE ExecutionEnvironment SUCCESS !")
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "FlinkLearnStreamDDLSQLEntry")
    bEnv = ExecutionEnvironment.createLocalEnvironment()
    batchEnv = BatchTableEnvironment.create(bEnv)
    streamEnv = FlinkEvnBuilder.buildStreamingEnv(
      FlinkLearnPropertiesUtil.param,
      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
      FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL)

    tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
      FlinkLearnPropertiesUtil.param,
      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
      FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL,
      Time.minutes(1),
      Time.minutes(6))
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
  }
//
//  /**
//    *
//    * @param topic
//    * @param broker
//    * @return
//    */
//  def kafkaSource(
//      topic: String,
//      broker: String,
//      reset: String): FlinkKafkaConsumer[KafkaTopicOffsetMsg] = {
//    val kafkasource = KafkaManager.getKafkaSource(
//      topic,
//      broker,
//      new TopicOffsetMsgDeserialize())
//    kafkasource.setCommitOffsetsOnCheckpoints(true)
//    if (reset eq "earliest") kafkasource.setStartFromEarliest //不加这个默认是从上次消费
//    else if (reset eq "latest") kafkasource.setStartFromLatest
//    kafkasource
//  }
//
//  def getKafkaDataStream(topic: String, broker: String, reset: String): DataStream[KafkaTopicOffsetMsg] =
//    streamEnv.addSource(kafkaSource(topic, broker, reset))
//
}
