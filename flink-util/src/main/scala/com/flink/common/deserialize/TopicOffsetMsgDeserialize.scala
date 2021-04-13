//package com.flink.common.deserialize
//
//import com.alibaba.fastjson.JSON
//import com.flink.common.kafka.KafkaManager.KafkaMessge
//import org.apache.commons.lang3.time.DateFormatUtils
//import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
//import org.apache.flink.streaming.api.scala.createTypeInformation
//import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
//import org.apache.kafka.clients.consumer.ConsumerRecord
//
//import java.text.SimpleDateFormat
//
//class TopicOffsetMsgDeserialize
//    extends KafkaDeserializationSchema[KafkaMessge] {
//  val smp = new SimpleDateFormat("yyyy-MM-dd HH:MM:SS")
//  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
//    val json = JSON.parseObject(new String(record.value()))
//    val rowtime = if(json.containsKey("rowtime")) json.getString("rowtime") else "1970-01-01 00:00:00"
//    KafkaMessge(
//      new String(record.topic()),
//      record.offset(),
//      smp.parse(rowtime).getTime,
//      if(json.containsKey("msg")) json.getString("msg") else null,
//      rowtime,
//      if(json.containsKey("uid")) json.getString("uid") else null)
//  }
//  override def isEndOfStream(nextElement: (KafkaMessge)) = {
//    false
//  }
//  override def getProducedType() = {
//    createTypeInformation[KafkaMessge]
//      .asInstanceOf[CaseClassTypeInfo[KafkaMessge]]
//
//  }
//}
