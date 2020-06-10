package com.flink.learn.entry

import com.flink.common.core.{
  EnvironmentalKey,
  FlinkEvnBuilder,
  FlinkLearnPropertiesUtil
}
import com.flink.common.core.FlinkLearnPropertiesUtil.{
  BROKER,
  FLINK_DEMO_CHECKPOINT_PATH,
  TEST_TOPIC,
  param
}
import org.apache.flink.streaming.api.scala._
import com.flink.common.deserialize.TopicOffsetMsgDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg
import com.flink.learn.richf.WordCountRichFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

object FlinkStreamJoinTest {
  var env: StreamExecutionEnvironment = null

  /**
    * 三个流的join，其中两个流是维表，将维表数据固定在state中等待join
    * @param args
    */
  def main(args: Array[String]): Unit = {

    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "LocalFlinkTest")
    env = FlinkEvnBuilder.buildStreamingEnv(param, FLINK_DEMO_CHECKPOINT_PATH) // 1 min

    // 只输出test2的数据，
    env
      .addSource(kafkaSource("test1,test2", BROKER))
      .map { x =>
        val arr = x.msg.split(",", -1)
        MsgInfo(x.topic, arr(1).toDouble, arr(0))
      }
      .keyBy("key")
      .flatMap(new RichFlatMapFunction[MsgInfo, MsgInfo] {
        var lastState: ValueState[MsgInfo] = _
        override def flatMap(value: MsgInfo, out: Collector[MsgInfo]): Unit = {
          if (value.topic == "test1") {
            lastState.update(value)
            println("test1 : ", lastState.value())
          } else {
             val v = lastState.value()
            println("test2 : ", v)
             if (v != null)
              out.collect(value.copy(price = value.price * v.price))
          }
        }
        override def open(parameters: Configuration): Unit = {
          val desc =
            new ValueStateDescriptor[MsgInfo]("MsgInfo",
                                              classOf[MsgInfo],
                                              MsgInfo(null, 0.0, null))
          lastState = getRuntimeContext().getState(desc)
        }
      })
      .print()

    env.execute()
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
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    kafkasource
  }
  case class MsgInfo(topic: String, price: Double, key: String)
}
