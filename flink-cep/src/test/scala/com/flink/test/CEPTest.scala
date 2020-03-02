package com.flink.test

import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.KafkaMessge
import com.flink.learn.entry.PropertiesUtil
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import scala.collection.JavaConversions._
object CEPTest {
  val TOPIC = "mobssprequestlog_p15"
  val BROKER =
    "10.6.161.208:9092,10.6.161.209:9092,10.6.161.210:9092,10.6.161.211:9092,10.6.161.212:9092"
  val checkPointPath =
    "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  def main(args: Array[String]): Unit = {
    PropertiesUtil.init("proPath");

    val kafkasource = new FlinkKafkaConsumer010[KafkaMessge](
      TOPIC.split(",").toList,
      new TopicMessageDeserialize(),
      KafkaManager.getKafkaParam(BROKER, "test"))
    kafkasource.setCommitOffsetsOnCheckpoints(false)
    kafkasource.setStartFromEarliest()//不加这个默认是从上次消费
    val env = FlinkEvnBuilder.buildStreamingEnv(PropertiesUtil.param, checkPointPath, 60000) // 1 min
    val result = env
      .addSource(kafkasource)
      .print()

    env.execute("lmq-flink-demo") //程序名
  }
}
