package com.flink.java.sourcesink.test

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.Cluster

import java.text.SimpleDateFormat

object KafkaUtilTest {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    kafkaAdmin()
  }

  val simp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //scalastyle:off

  /**
   *
   * @param properties
   */
  def sendRecord(): Unit = {
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
    // 1626537303000
    // 1626623703000
    for (i <- 1 to 10000) {
      producer.send(new ProducerRecord[String, String](
        "test",
        s"""{"plat":1,"duid":"ddd${i}","appkey":"a","serdatetime":1626623703000}"""
      ))
      producer.send(new ProducerRecord[String, String](
        "test",
        s"""{"plat":1,"duid":"ddd${i}","appkey":"b","serdatetime":1626623703000}"""
      ))
      // Thread.sleep(500)
    }
    println(">>>>>>>>>. end ")
    producer.close()

  }

  def sendTest(): Unit = {

    val kafkabroker = "10.90.45.16:9092,10.90.45.17:9092,10.90.45.18:9092,10.90.45.19:9092,10.90.45.20:9092,10.90.45.21:9092,10.90.45.22:9092"
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
    // 1626537303000
    // 1626623703000
    for (i <- 1 to 2) {
      producer.send(new ProducerRecord[String, String](
        "test",
        s"""{"plat":1,"duid":"ddd${i}","appkey":"test","serdatetime":1626793409045}"""
      ))
      // Thread.sleep(500)
    }
    println(">>>>>>>>>. end ")
    producer.close()

  }


  def kafkaAdmin(): Unit ={
    val kafkabroker = "10.90.45.16:9092,10.90.45.17:9092,10.90.45.18:9092,10.90.45.19:9092,10.90.45.20:9092,10.90.45.21:9092,10.90.45.22:9092"
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
//    import org.apache.kafka.clients.admin.KafkaAdminClient
//    val adminClient = new KafkaAdminClient(map)
//    adminClient.listTopics().names().get().forEach(x => {println(x)})
  }
}
