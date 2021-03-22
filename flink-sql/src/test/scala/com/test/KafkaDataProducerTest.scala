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
         |  "apppkg": "cn.sharesdk.demo",
         |  "appver": "3.8.2",
         |  "brower": "QQ浏览器",
         |  "carrier": -1,
         |  "categoryId": -1,
         |  "city": "上海",
         |  "clickDomain": "f7mf.l.urlcloud.sharesdk.test.mob.com",
         |  "country": "中国",
         |  "date": 1615876280063,
         |  "datetime": 1615876280063,
         |  "day": 16,
         |  "developerId": 1,
         |  "deviceId": "aaaaaaaaaaaaaaa",
         |  "domain": "l.urlcloud.sharesdk.test.mob.com",
         |  "domainKey": "75021d9e35849b92f6f500a99754b76d0c5aa54f",
         |  "factory": "",
         |  "hour": 14,
         |  "id": "605050b85bff74e98946ac92",
         |  "ipAddr": "58.34.35.28",
         |  "key": "moba0b0c0d0",
         |  "language": "zh-cn",
         |  "mappingId": 535103734427308032,
         |  "model": "",
         |  "month": 3,
         |  "network": "wifi",
         |  "os": "Android",
         |  "plat": 5,
         |  "province": "上海",
         |  "publishCarrier": -1,
         |  "publishCity": "上海",
         |  "publishCountry": "中国",
         |  "publishIpAddr": "58.34.35.28",
         |  "publishProvince": "上海",
         |  "screensize": "",
         |  "sdkver": 30805,
         |  "sysplat": 1,
         |  "sysver": "",
         |  "terminalType": 2,
         |  "uid": "",
         |  "url": "http://download.sdk.mob.com/2021/01/25/16/16115618066761.02.html",
         |  "year": 2021
         |}""".stripMargin
    )
    //          println(i, s)
    producer.send(s)
    producer.close()
    //          Thread.sleep(1000)
    //        }
  }
}
