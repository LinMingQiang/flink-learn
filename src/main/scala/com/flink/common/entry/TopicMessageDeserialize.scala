package com.flink.common.entry

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

class TopicMessageDeserialize extends KafkaDeserializationSchema[(String,String)]{
 override def deserialize(record:ConsumerRecord[Array[Byte],Array[Byte]])={
   (record.topic,new String(record.value()))
 }
 override def isEndOfStream(nextElement:(String,String))={
   false
 }
 override def getProducedType()={
  BasicTypeInfo.getInfoFor(classOf[Tuple2[String,String]])
 }
}
