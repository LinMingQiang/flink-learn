package com.test

import java.util.Date

import org.apache.kafka.clients.producer.KafkaProducer

object KafkaDataProducerTest {
  def main(args: Array[String]): Unit = {
    val kafkabroker = "localhost:9092"
    // val kafkabroker =
    // "10.6.161.208:9092,10.6.161.209:9092,10.6.161.210:9092,10.6.161.211:9092,10.6.161.212:9092"
    val map = new java.util.HashMap[String, Object]
    map.put("bootstrap.servers", kafkabroker)
    map.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    map.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    map.put("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    map.put("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    map.put("group.id", "test");
    map.put("enable.auto.commit", "false")
    val producer = new KafkaProducer[String, String](map)
    import org.apache.kafka.clients.producer.ProducerRecord
    var i = 0
    while (true) {
      i += 1
      val s = new ProducerRecord[String, String](
        "test",
        System.currentTimeMillis().toString,
        s"""{"rowtime":"2020-01-01 00:00:01","msg":"${i}"}""".stripMargin
      )
      println(i, s)
      producer.send(s)
//      producer.close()
      Thread.sleep(10)
    }
  }
}
