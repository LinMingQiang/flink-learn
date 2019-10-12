package com.flink.common.entry

import com.alibaba.fastjson.JSON
import org.apache.flink.streaming.api.windowing.assigners.{ProcessingTimeSessionWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._
object FlinkWindowTest {
  val checkpointPath =
    "file:///C:\\Users\\mqlin\\Desktop\\testdata\\flink\\rocksdbcheckpoint"
  def main(args: Array[String]): Unit = {
    val env = getFlinkEnv(checkpointPath, 3000) // 1 min
    val kafkasource = new FlinkKafkaConsumer010[KafkaMessge](
      TOPIC.split(",").toList,
      new TopicMessageDeserialize(),
      getKafkaParam(BROKER))
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromEarliest() //不加这个默认是从上次消费
    val result = env
      .addSource(kafkasource)
      .map(x => (JSON.parseObject(x.msg).getString("dist"), 1))
      .filter(_._1 == "2412")
      .keyBy(0)
      // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // = .timeWindow(Time.seconds(60))
      //
      .sum(1)
      .print
    env.execute("jobname")
  }
}
