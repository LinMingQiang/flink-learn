package com.flink.common.entry

import com.alibaba.fastjson.JSON
import com.flink.common.deserialize.{TopicMessageDeserialize, TopicOffsetMsgDeserialize}
import com.flink.common.sink.OperatorStateBufferingSink
import org.apache.flink.streaming.api.scala._
object FlinkOperatorStateTest {
  val checkpointPath = "file:///C:\\Users\\mqlin\\Desktop\\testdata\\flink\\checkpoint\\FlinkOperatorStateTest"
  def main(args: Array[String]): Unit = {
    val env = getFlinkEnv(checkpointPath, 60000) // 1 min
    val kafkasource = getKafkaSource(TOPIC, BROKER,  new TopicOffsetMsgDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    env
      .addSource(kafkasource)
      .map(x => (JSON.parseObject(x.msg).getString("dist"), 1))
      .keyBy(0)
      .sum(1)
      .addSink(new OperatorStateBufferingSink(1))

    env.execute("FlinkOperatorStateTest")
  }
}
