package com.flink.learn.entry

import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.core.FlinkLearnPropertiesUtil.{CHECKPOINT_PATH, param}
import com.flink.common.deserialize.KafkaMessageDeserialize
import org.apache.flink.streaming.api.scala._
import com.flink.common.kafka.KafkaManager
import com.flink.learn.richf.WordCountRichFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object FlinkStreamCustomJoinTest {
  var env: StreamExecutionEnvironment = null

  /**
    * 三个流的join，其中两个流是维表，将维表数据固定在state中等待join。
    * @param args
    */
  def main(args: Array[String]): Unit = {

    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "LocalFlinkTest")
    env = FlinkEvnBuilder.buildStreamingEnv(param, CHECKPOINT_PATH) // 1 min

    // 只输出test2的数据，
    env
      .addSource(KafkaManager.getKafkaSource("test1,test2", FlinkLearnPropertiesUtil.BROKER, new KafkaMessageDeserialize))
      .map { x =>
        val arr = x.msg.split(",", -1)
        val uid = arr(0)
        val price = arr(1).toDouble
        MsgInfo(x.topic, uid = uid, price = price)
      }
      .keyBy("uid")
      .flatMap(new RichFlatMapFunction[MsgInfo, MsgInfo] {
        var lastState: ValueState[MsgInfo] = _
        override def flatMap(value: MsgInfo, out: Collector[MsgInfo]): Unit = {
          if (value.topic == "test1") { // 维表
            lastState.update(value)
          } else { // 主数据
            val v = lastState.value()
            if (v != null) { // 没join到
              out.collect(value.copy(price = value.price * v.price))
            } else {
              out.collect(value)
            }
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
  case class MsgInfo(topic: String, price: Double, uid: String)
}
