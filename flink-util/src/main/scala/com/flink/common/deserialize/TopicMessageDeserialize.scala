package com.flink.common.deserialize

import com.flink.common.kafka.KafkaManager.KafkaMessge
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{createTypeInformation => _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

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


/** flink 1.7.2 */
//import org.apache.flink.api.common.typeinfo.TypeInformation
//import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
//import org.apache.flink.api.java.typeutils.TypeExtractor.getForClass
//import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
//import org.apache.flink.streaming.api.scala.createTypeInformation
//class KafkaKeyValueDeserializationSchema
//  extends KeyedDeserializationSchema[KafkaKeyValue] {
//  override def deserialize(messageKey: Array[Byte],
//                           message: Array[Byte],
//                           topic: String,
//                           partition: Int,
//                           offset: Long): KafkaKeyValue = {
//    KafkaKeyValue(topic, new String(messageKey), new String(message))
//  }
//
//  override def isEndOfStream(t: KafkaKeyValue): Boolean = false
//
//  override def getProducedType: TypeInformation[KafkaKeyValue] =
//    createTypeInformation[KafkaKeyValue].asInstanceOf[CaseClassTypeInfo[KafkaKeyValue]]
//    TypeInformation.of(
//      KafkaMessge.getClass.asInstanceOf[Class[KafkaMessge]])
//}




