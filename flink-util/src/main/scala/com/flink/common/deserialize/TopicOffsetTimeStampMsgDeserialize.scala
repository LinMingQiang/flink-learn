package com.flink.common.deserialize

import com.flink.common.kafka.KafkaManager._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicOffsetTimeStampMsgDeserialize
  extends KafkaDeserializationSchema[KafkaTopicOffsetTimeMsg] {
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    KafkaTopicOffsetTimeMsg(new String(record.topic()),
      record.offset(),
      new String(record.value()).toLong,
      new String(record.value()))
  }

  override def isEndOfStream(nextElement: KafkaTopicOffsetTimeMsg) = {
    false
  }

  override def getProducedType() = {
    createTypeInformation[KafkaTopicOffsetTimeMsg]
      .asInstanceOf[CaseClassTypeInfo[KafkaTopicOffsetTimeMsg]]

  }
}
