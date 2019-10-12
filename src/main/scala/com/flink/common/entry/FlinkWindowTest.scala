package com.flink.common.entry

import com.alibaba.fastjson.JSON
import com.flink.common.deserialize.TopicMessageDeserialize
import org.apache.flink.streaming.api.scala._
object FlinkWindowTest {
  val checkpointPath =
    "file:///C:\\Users\\mqlin\\Desktop\\testdata\\flink\\checkpoint\\FlinkWindowTest"
  def main(args: Array[String]): Unit = {
    val env = getFlinkEnv(checkpointPath, 3000) // 1 min
    val kafkasource = getKafkaSource(TOPIC, BROKER,  new TopicMessageDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    val result = env
      .addSource(kafkasource)
       .map(x => (JSON.parseObject(x.msg).getString("dist"), 1))
       .keyBy(0)
      // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      // .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // = .timeWindow(Time.seconds(60))
      //
       .sum(1)
      .print
    env.execute("jobname")
  }
}
