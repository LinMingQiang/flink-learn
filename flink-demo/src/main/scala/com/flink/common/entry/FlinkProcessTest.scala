package com.flink.common.entry

import com.alibaba.fastjson.JSON
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.deserialize.TopicOffsetMsgDeserialize
import com.flink.common.kafka.KafkaManager
import com.flink.common.richf.CountWithTimeoutProcessFunction
import com.flink.common.sink.OperatorStateBufferingSink
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConversions._

object FlinkProcessTest {
  // 可以实现类似于 session 的功能， 某个key在某个session里面的访问次数。例如 session的间隔为 60s
  val checkpointPath = "file:///C:\\Users\\mqlin\\Desktop\\testdata\\flink\\checkpoint\\FlinkProcess"
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildFlinkEnv(checkpointPath, 60000) // 1 min
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    val kafkasource = KafkaManager.getKafkaSource(TOPIC, BROKER,  new TopicOffsetMsgDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
   env
      .addSource(kafkasource)
      .map(x => (JSON.parseObject(x.msg).getString("dist"),""))
      .keyBy(0)
      .process(new CountWithTimeoutProcessFunction)
      .print
    env.execute("FlinkOperatorStateTest")
  }
}
