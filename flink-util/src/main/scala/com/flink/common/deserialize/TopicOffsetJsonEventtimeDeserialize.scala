package com.flink.common.deserialize

import com.alibaba.fastjson.{JSON, JSONObject}
import com.flink.common.kafka.KafkaManager._
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicOffsetJsonEventtimeDeserialize
  extends KafkaDeserializationSchema[KafkaTopicOffsetTimeMsg] {
  // {"ts":1,"msg":"1"}
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    val msg = new String(record.value())
    val msgJson = JSON.parseObject(msg)
      if(msgJson.containsKey("ts")) {
        KafkaTopicOffsetTimeMsg(new String(record.topic()),
          record.offset(),
          1600000020000L + msgJson.getLong("ts")*1000,
          DateFormatUtils.format(1600000020000L + msgJson.getLong("ts")*1000, "yyyy-mm-dd HH:mm:ss"),
          msgJson.getString("msg"))
      } else {
        null
      }
  }

  override def isEndOfStream(nextElement: KafkaTopicOffsetTimeMsg) = {
    false
  }

  override def getProducedType() = {
    createTypeInformation[KafkaTopicOffsetTimeMsg]
      .asInstanceOf[CaseClassTypeInfo[KafkaTopicOffsetTimeMsg]]
  }
}
