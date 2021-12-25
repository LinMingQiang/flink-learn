package com.flink.common.deserialize

import com.alibaba.fastjson.JSON
import com.flink.common.kafka.KafkaManager.KafkaMessge
import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.api.scala.{createTypeInformation => _}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.text.SimpleDateFormat

class KafkaMessageDeserialize extends KafkaDeserializationSchema[KafkaMessge] {
  val timeFormatPattern = "yyyy-MM-dd hh:mm:ss"
  val smp = new SimpleDateFormat(timeFormatPattern)

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]],
                           out: Collector[KafkaMessge]): Unit = {
    if (new String(record.value()).startsWith("{")) {
      val json = JSON.parseObject(new String(record.value()))
      val rowtime = {
        if (json.containsKey("rowtime")) json.getString("rowtime")
        else "2020-01-01 00:00:00"
      }
      val msg = KafkaMessge(
        new String(record.topic()),
        record.offset(),
        smp.parse(rowtime).getTime,
        if (json.containsKey("msg")) json.getString("msg") else null,
        rowtime,
        if (json.containsKey("uid")) json.getString("uid") else null
      )
      out.collect(msg)
    } else {
      if (new String(record.value()).nonEmpty) {
        val tim = System.currentTimeMillis()
        out.collect(
          KafkaMessge(
            new String(record.topic()),
            record.offset(),
            tim,
            new String(record.value()),
            DateFormatUtils.format(tim, timeFormatPattern),
            null
          ))
      }
    }
  }
  override def isEndOfStream(nextElement: (KafkaMessge)) = {
    false
  }

  override def getProducedType() = {
    createTypeInformation[KafkaMessge]
      .asInstanceOf[CaseClassTypeInfo[KafkaMessge]]

  }

  override def deserialize(
      consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): KafkaMessge =
    ???
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
