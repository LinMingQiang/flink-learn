package com.flink.common.entry
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions._
import java.util.Properties

import com.flink.common.bean.{AdlogBean, StatisticalIndic}
import com.flink.common.richf.{
  AdlogPVRichFlatMapFunction,
  AdlogPVRichMapFunction,
  WordCountRichFunction
}
import com.flink.common.sink.{
  HbaseReportSink,
  StateRecoverySinkCheckpointFunc,
  SystemPrintSink
}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{
  StateTtlConfig,
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.util.Collector

object KafkaWordCountTest {
  val BROKER =
    "xxx"
  val TOPIC = "xx"
  val cp =
    "file:///C:\\Users\\mqlin\\Desktop\\testdata\\flink\\rocksdbcheckpoint"
  def main(args: Array[String]): Unit = {
    val env = getFlinkEnv(cp, 300000) // 1 min
    // 同时支持多个流地运行
    val impressDstream = getImpressDStream(env)
    val clickDStream = getClickDStream(env)
    clickDStream.print
    impressDstream.addSink(new SinkFunction[(String, Int)] {
      override def invoke(value: (String, Int)): Unit = {
        println(value)
      }
    })
    env.execute()
  }

  /**
    *
    * @param env
    * @return
    */
  def getImpressDStream(env: StreamExecutionEnvironment) = {
    val kafkasource2 = new FlinkKafkaConsumer010[(KafkaMessge)](
      "mobdspconvlog".split(",").toList,
      new TopicMessageDeserialize(),
      getKafkaParam(BROKER))
    kafkasource2.setCommitOffsetsOnCheckpoints(true)
    kafkasource2.setStartFromEarliest() //不加这个默认是从上次消费
    env
      .addSource(kafkasource2)
      .flatMap(_.topic.split("\\|", -1))
      .map(x => (x, 1))
      .keyBy(0)
      .flatMap(new WordCountRichFunction)
  }

  /**
    *
    * @param env
    * @return
    */
  def getClickDStream(env: StreamExecutionEnvironment) = {
    val kafkasource = new FlinkKafkaConsumer010[(KafkaMessge)](
      TOPIC.split(",").toList,
      new TopicMessageDeserialize(),
      getKafkaParam(BROKER))
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromEarliest() //不加这个默认是从上次消费
    env
      .addSource(kafkasource)
      .flatMap(_.topic.split("\\|", -1))
      .map(x => (x, 1))
      .keyBy(0)
      .flatMap(new WordCountRichFunction)
  }
}
