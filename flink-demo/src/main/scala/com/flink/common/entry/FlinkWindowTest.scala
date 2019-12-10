package com.flink.common.entry

import com.alibaba.fastjson.JSON
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.param.PropertiesUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
object FlinkWindowTest {
  // 类似于分批。统计每个窗口内的数据
  PropertiesUtil.init("proPath");

  val checkpointPath =
    "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildFlinkEnv(PropertiesUtil.param, checkpointPath, 3000) // 1 min
    val kafkasource =
      KafkaManager.getKafkaSource(TOPIC, BROKER, new TopicMessageDeserialize())
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
