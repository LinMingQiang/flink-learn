package com.flink.common.deserialize

import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicOffsetMsgDeserialize
    extends KafkaDeserializationSchema[KafkaTopicOffsetMsg] {
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    (KafkaTopicOffsetMsg(new String(record.topic()),
                         record.offset(),
                         new String(record.value())))
  }
  override def isEndOfStream(nextElement: (KafkaTopicOffsetMsg)) = {
    false
  }
  override def getProducedType() = {
    createTypeInformation[KafkaTopicOffsetMsg]
      .asInstanceOf[CaseClassTypeInfo[KafkaTopicOffsetMsg]]

  }
}
