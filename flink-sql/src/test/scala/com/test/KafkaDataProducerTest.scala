package com.test

import java.util.Date

import org.apache.kafka.clients.producer.KafkaProducer

object KafkaDataProducerTest {
  def main(args: Array[String]): Unit = {
    val kafkabroker = "10.21.33.28:9092,10.21.33.29:9092,10.21.131.11:9092"
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
    //        var i = 0
    //        while (true) {
    //          i += 1
    val s = new ProducerRecord[String, String](
      "backflowlog",
      System.currentTimeMillis().toString,
      s"""{
         |    "apppkg":"cn.sharesdk.demo",
         |    "appver":"3.8.2",
         |    "brower":"Chrome",
         |    "carrier":-1,
         |    "categoryId":-1,
         |    "city":"",
         |    "clickDomain":"f7mf.l.urlcloud.sharesdk.test.mob.com",
         |    "country":"保留地址",
         |    "date":1620230400000,
         |    "day":6,
         |    "developerId":1,
         |    "deviceId":"194dd57c8f10e7404061c1455e1fdb9295c683f7",
         |    "domain":"f7mf.l.urlcloud.sharesdk.test.mob.com",
         |    "domainKey":"75021d9e35849b92f6f500a99754b76d0c5aa54f",
         |    "factory":"HUAWEI",
         |    "hour":14,
         |    "id":"6093889f6d89e58eb02d1225",
         |    "ipAddr":"172.25.70.248",
         |    "key":"moba0b0c0d0",
         |    "language":"zh-cn",
         |    "mappingId":553581743735062528,
         |    "model":"HMA-AL00",
         |    "month":5,
         |    "network":"wifi",
         |    "os":"Windows ME",
         |    "plat":24,
         |    "province":"保留地址",
         |    "publishCarrier":-1,
         |    "publishCity":"",
         |    "publishCountry":"",
         |    "publishProvince":"",
         |    "screensize":"1080x2244",
         |    "sdkver":30805,
         |    "sysplat":1,
         |    "sysver":"29",
         |    "terminalType":1,
         |    "uid":"",
         |    "url":"http://download.sdk.mob.com/2021/01/25/16/16115618066761.02.html",
         |    "year":2021
         |}""".stripMargin
    )
    //          println(i, s)
    producer.send(s)
    producer.close()
    //          Thread.sleep(1000)
    //        }
  }
}
