package com.flink.learn.entry

import com.alibaba.fastjson.JSON
import com.flink.common.core.FlinkLearnPropertiesUtil.{FLINK_DEMO_CHECKPOINT_PATH, param}
import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.core.FlinkLearnPropertiesUtil._
import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.kafka.KafkaManager
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
object FlinkWindowTest {
  // 类似于分批。统计每个窗口内的数据
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
      "KafkaWordCountTest")
    val env = FlinkEvnBuilder.buildStreamingEnv(param,
      FLINK_DEMO_CHECKPOINT_PATH,
      10000) // 1 min
    val kafkasource =
      KafkaManager.getKafkaSource(TEST_TOPIC, BROKER, new TopicMessageDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromEarliest() //不加这个默认是从上次消费
    val result = env
      .addSource(kafkasource)
      .map(x => (x.msg.split("|")(7), 1))
      .setParallelism(1)
      .keyBy(0)
      // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))) // 算session
      .window(TumblingProcessingTimeWindows.of(Time.seconds(4))) // = .timeWindow(Time.seconds(60))
      .sum(1) // 10s窗口的数据
      .print
      // .setParallelism(2)

    env.execute("jobname")
  }
}
