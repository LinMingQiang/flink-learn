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
    sendLocal()
  }

val simp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  //scalastyle:off
  /**
    *
    * @param properties
    */
  def sendRecord(): Unit = {
    val kafkabroker = "10.21.33.28:29092,10.21.33.29:29092,10.21.33.11:29092"
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

//    while (true) {
      val producer = new KafkaProducer[String, String](map)
      import org.apache.kafka.clients.producer.ProducerRecord
       val s = simp.parse("2021-04-03 13:17:20")
       println(s.getTime, DateFormatUtils.format(s, "yyyy-MM-dd HH:mm:ss"))
      // "2021-04-01 11:11:11"
      // 1614670811000
      val adx = new ProducerRecord[String, String](
        "ad_adx_response_log",
        s"""{"os":2,"carrier":-922637440,"appkey":"over123456","log_type":8,"sys_time":"${DateFormatUtils.format(s, "yyyy-MM-dd HH:mm:ss")}","sys_interval":0,"req_id":"tuomintest","seller_id":0,"bidfloor":200000,"app_id":"1296713728361689090","adslot_id":"1296713748993470465","ua":"Mozilla/5.0 (Linux; Android 6.0; HUAWEI MLA-AL10 Build/HUAWEIMLA-AL10; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/55.0.2883.91 Mobile Safari/537.36","ipv4":"172.25.48.69","gacode":1000000000,"device_type":0,"make":"HUAWEI","osv":"6.0","language":"zh","connectiontype":"1","didmd5":"dd895e6923098fbc0254fa3167b406af","lat":"31.178812","lon":"121.412102","isfill":2,"buyerid":105,"advid":1286183056149241858,"response_price":148,"creative_type":2,"template_id":81,"policy_id":1296713749228351490,"adx_req_id":"499215771900497920","ssp_err_code":200,"all_adx_id":"105","all_adx_bid_status":"2","all_adx_err_code":"200","all_buyer_type":"2","all_buyer_adv_id":"1286183056149241858","all_buyer_creative_id":"1315942824011030530","is_test":"0","platform_type":"1","ieid":"dd895e6923098fbc0254fa3167b406af","appver":"1.0.0","sysverint":"23","apppkg":"com.over","mcid":"7d39c297a2fe00f3aaa3a75bd4b4af4f","snid":"1d403e2e81b9a73eea2fbc4704633a58","sdkver":"2.1.4","all_adx_slot_id":"null","all_adx_bid_price":"150","all_adx_app_id":"null"}"""
      )
      //    val imp = new ProducerRecord[String, String](
      //      "ad_ssp_impression_log",
      //      s"""{"ipv4":"172.25.62.63","make":"HUAWEI","model":"HUAWEIMLA-AL10","os":"2","appkey":"over123456","appver":"1.0.0","apppkg":"com.over","sdkver":"2.1.4","sys_time":${System.currentTimeMillis()},"req_time":1616468756636,"log_type":10,"req_id":"537277250801119232","selling_price":50,"buyerid":105,"app_id":"1296713728361689090","adslot_id":"1296713755490447361","gacode":"1000000000","device_type":0,"policy_id":1296713755649830914,"buyer_type":"2","ad_type":"8","is_test":"0","ieid":"9e97a4fdab68153b77395ed228a1483d","mcid":"7d39c297a2fe00f3aaa3a75bd4b4af4f","snid":"1d403e2e81b9a73eea2fbc4704633a58"}"""
      //    )

      //    val click = new ProducerRecord[String, String](
      //      "ad_ssp_click_log",
      //      s"11|${new Date().getTime}|0|0||09154bebf9677d2b1642912e62ad826dad606f96|1|100|0|11|0|1239408847917228033|1239409112732999682|183.185.59.82|1156140100|0||vivo-Y85|2|4b0fcdc6b4cc7616d9ee4fdf98ba0c7c529fd206|2a11d9ca5accb4e6e5f833dffa12ba01|15dad567c25a6fce3c9fbca74ae74ccbdf86fa96|33c5644d77d47218b41f63f412f35755|376ac9ee30bf218a|1242040821670199298|1|8|0||868740038822336|||2.1.5||com.youhessp.zhangyu|4C:C0:0A:10:29:0D|||12497|oaid"
      //    )
      //   producer.send(imp)
      // producer.send(click)
      //    producer.send(adx)
      producer.send(adx)
      producer.close()
//      Thread.sleep(500)
//    }
  }



  def sendLocal(): Unit ={
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

    for(i <- 1 to 100) {
      val adx = new ProducerRecord[String, String](
        "test",
        s"""{"serdatetime":"${System.currentTimeMillis()}",
           |"duid":"duid${i}",
           |"sdks":[{"k":"30007"}],
           |"appkey":"appkey",
           |"deviceid":"123456d890111111111111111111111111111124",
           |"plat":"2"}""".stripMargin
      )
      println(adx)
      producer.send(adx)
    }
    producer.close()
  }
}
