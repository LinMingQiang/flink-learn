package com.flink.common.deserialize

import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.common.kafka.KafkaManager._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicOffsetEventTimeMsgDeserialize
  extends KafkaDeserializationSchema[KafkaTopicOffsetMsgEventtime] {
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    val msg = new String(record.value())
    val msgJson = JSON.parseObject(msg)
      if(msgJson.containsKey("etime")) {
        KafkaTopicOffsetMsgEventtime(new String(record.topic()),
          record.offset(),
          msg,
          msgJson.getLong("etime"))
      } else {
        null
      }
  }

  override def isEndOfStream(nextElement: KafkaTopicOffsetMsgEventtime) = {
    false
  }

  override def getProducedType() = {
    createTypeInformation[KafkaTopicOffsetMsgEventtime]
      .asInstanceOf[CaseClassTypeInfo[KafkaTopicOffsetMsgEventtime]]
  }
}
