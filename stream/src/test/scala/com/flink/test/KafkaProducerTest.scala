package com.flink.test

import java.util.Date

import org.apache.kafka.clients.producer.KafkaProducer

object KafkaProducerTest {
  def main(args: Array[String]): Unit = {
    val kafkabroker = "localhost:9092"
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
      val reqid = s"reqid_${(Math.random() * 1000000).toInt}"
      val username = s"username_${(Math.random() * 10).toInt}"
      val price = (Math.random() * 100)
      val s = new ProducerRecord[String, String](
        "adrequestlog",
        s"""${new Date().getTime}|${reqid}|${username}|${price}"""
      )
      println(i, s)
      producer.send(s)
      Thread.sleep(1000)
    }
  }
}
