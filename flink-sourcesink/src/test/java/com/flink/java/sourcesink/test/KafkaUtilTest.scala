package com.flink.java.sourcesink.test

import org.apache.commons.lang.time.DateFormatUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.text.SimpleDateFormat

object KafkaUtilTest {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    sendRecord()
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


  def sendLocal(): Unit = {
    //    val kafkabroker = "10.21.33.28:29092,10.21.33.29:29092,10.21.131.11:29092"
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

    // 1.1 1609513200000
    // 1.2 1609599600000
    // 1.3 1609686000000
    // 1.4 1609772400000
    // 1.5 1609858800000
    // 1.8 1610118000000
    // 1.9 1610204400000
    // 1.15 1610722800000
    // pv_test
    for (i <- 1 to 1) {
      val adx = new ProducerRecord[String, String](
        "test",
        null.asInstanceOf[Integer],
        1L,
        null,
        s"""{"serdatetime":"1623206516000",
           |"duid":"duidsd2d",
           |"sdks":[{"k":"30007"}],
           |"appkey":"appkey2",
           |"deviceid":"123356d890121111111111111111111111111124",
           |"plat":"2"}""".stripMargin
      )
      println(adx)

      producer.send(adx)
    }
    producer.close()
  }
}
