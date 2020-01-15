package com.flink.learn.entry

import java.util.Date

import com.alibaba.fastjson.JSON
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.deserialize.TopicOffsetMsgDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.learn.bean.CaseClassUtil
import com.flink.learn.richf.SessiontProcessFunction
import com.flink.learn.param.PropertiesUtil
import com.flink.learn.sink.OperatorStateBufferingSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._

object FlinkProcessTest {
  // PropertiesUtil.init("proPath");
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildStreamingEnv(PropertiesUtil.param, cp, 60000) // 1 min
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    //    val source = KafkaManager.getKafkaSource(
//      TOPIC,
//      BROKER,
//      new TopicOffsetMsgDeserialize())
//    kafkasource.setCommitOffsetsOnCheckpoints(true)
//    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    // env
    //      .addSource(source)
    env
      .socketTextStream("localhost", 9876)
      .map(x => CaseClassUtil.SessionLogInfo(x, new Date().getTime))
      .keyBy(_.sessionId)
      .process(new SessiontProcessFunction)
      .print
    env.execute("FlinkOperatorStateTest")
  }
}
