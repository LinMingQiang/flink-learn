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
    sendTest()
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

    val kafkabroker = "10.89.120.11:29092,10.89.120.12:29092,10.89.120.16:29092"
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

    val topic = "dsp_ad_push_deliver_test"

    val producer = new KafkaProducer[String, String](map)
    import org.apache.kafka.clients.producer.ProducerRecord
    // 1626537303000
    // 1626623703000
    for (i <- 1 to 2) {
      producer.send(new ProducerRecord[String, String](
        topic,
        s"""{"appkey":"2d7ce93a26944","price":0,"ipv4":"172.25.64.179","ipv6":"__IPV6__","make":"__MAKE__","model":"__MODEL__","os":"__OS__","osv":"__OSV__","carrier":"__CARRIER__","duid":"8b0131e2b541cde85ed009b110e809e47379d079","sys_time":1628847081958,"req_time":1628847081406,"log_type":101,"req_id":"589507252164120576","adv_id":"202108061014","ad_plan_id":"202108061019","ad_activity_id":"202108131112","work_id":"4bplv5qsth9qiyh6gw","ad_creativity_id":"202108091031","ga_code":-1}"""
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
