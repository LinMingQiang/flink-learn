package com.flink.common.entry
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaMessge
import com.flink.common.param.PropertiesUtil
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._
import com.flink.common.richf.WordCountRichFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

object KafkaWordCountTest {
  PropertiesUtil.init("proPath");
  val cp =
    "file:///C:\\Users\\mqlin\\Desktop\\testdata\\flink\\rocksdbcheckpoint"
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildFlinkEnv(PropertiesUtil.param, cp, 10000) // 1 min
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
      "testimpress".split(",").toList,
      new TopicMessageDeserialize(),
      KafkaManager.getKafkaParam(BROKER))
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
      KafkaManager.getKafkaParam(BROKER))
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
