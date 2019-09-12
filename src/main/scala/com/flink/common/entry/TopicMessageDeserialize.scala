package com.flink.common.entry


import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.{createTypeInformation => _}
class TopicMessageDeserialize
    extends KafkaDeserializationSchema[(KafkaMessge)] {
  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    (KafkaMessge(new String(record.topic()), new String(record.value())))
  }
  override def isEndOfStream(nextElement: (KafkaMessge)) = {
    false
  }
  override def getProducedType() = {
    createTypeInformation[KafkaMessge].asInstanceOf[CaseClassTypeInfo[KafkaMessge]]
  }

}
