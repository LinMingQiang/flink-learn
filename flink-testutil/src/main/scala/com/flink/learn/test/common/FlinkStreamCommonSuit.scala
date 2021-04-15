package com.flink.learn.test.common

import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.core.FlinkLearnPropertiesUtil.{param, CHECKPOINT_PATH}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class FlinkStreamCommonSuit extends FunSuite with BeforeAndAfterAll {
  var env: StreamExecutionEnvironment = null
  protected override def beforeAll(): Unit = {
    super.beforeAll()
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "FlinkStreamCommonSuit")
    //    kafkasource.setStartFromSpecificOffsets(
    //      Map(new KafkaTopicPartition("maxwell_new", 0) -> 1L.asInstanceOf[java.lang.Long]));
    env = FlinkEvnBuilder.buildStreamingEnv(param,
      CHECKPOINT_PATH,
      1000000) // 1 min
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
  }

//  /**
//    *
//    * @param topic
//    * @param broker
//    * @return
//    */
//  def kafkaSource(
//      topic: String,
//      broker: String,
//      reset: String = "latest"): FlinkKafkaConsumer[KafkaTopicOffsetMsg] = {
//    val kafkasource = KafkaManager.getKafkaSource(
//      topic,
//      broker,
//      new TopicOffsetMsgDeserialize())
//    kafkasource.setCommitOffsetsOnCheckpoints(true)
//    reset match{
//      case "earliest" =>kafkasource.setStartFromEarliest() //不加这个默认是从上次消费
//      case "latest" => kafkasource.setStartFromLatest() //不加这个默认是从上次消费
//      case _ =>
//    }
//
//    kafkasource
//  }
}
