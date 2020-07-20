package com.test

import org.apache.kafka.clients.producer.KafkaProducer

object KafkaUtilTest {

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // val kafkabroker = "10.21.33.28:29092,10.21.33.29:29092,10.21.131.11:29092"
    val kafkabroker = "10.21.33.28:9092,10.21.33.29:9092"
    // val kafkabroker =
      // "10.6.161.208:9092,10.6.161.209:9092,10.6.161.210:9092,10.6.161.211:9092,10.6.161.212:9092"
    val map = new java.util.HashMap[String, Object]
    map.put("bootstrap.servers", kafkabroker)
    map.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    map.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    map.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
    map.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer")
    map.put("group.id", "test");
    map.put("enable.auto.commit", "false")
    // val ku = KafkaUtil(map)
    //    val o27 =
    //      ku.offsetsForTimes(Set("bgm_ad_sdk_req_log_p15"), 1577376000000L)
    //    val o28 =
    //      ku.offsetsForTimes(Set("bgm_ad_sdk_req_log_p15"), 1577462400000L)
    //    import scala.collection.JavaConverters._
    //    val sum = o28.asScala.map {
    //      case (tp, l) =>
    //        l.offset() - o27.get(tp).offset()
    //    }.sum
    //    println(sum)

    sendRecord(map)
    // println(ku.getLastestOffset(Set("mobssprequestlog_p15")))
    // println(ku.getLastestOffset(Set("mobssprequestlog_p15")))
    // println(ku.getLastestOffset(Set("mobssprequestlog_p15")))
    // println(ku.getLastestOffset(Set("sharesdk_run")))
    // println(ku.getEarleastOffset(Set("mobssprequestlog")))

    // println(ku.getLastestOffset(Set("marketplus_frontend_req_log_p10_2")))
    //    val tbl = HbaseHandler
    //      .getHbaseConn("ZOOKEEPER")
    //      .getTable(TableName.valueOf("CHECK_TBL"))
    //    HbaseHandler.setConsumterOffset(tbl, "","","","")
  }

  /**
   *
   * @param properties
   */
  def sendRecord(properties: java.util.HashMap[String, Object]): Unit = {
    val producer = new KafkaProducer[String, String](properties)
    import org.apache.kafka.clients.producer.ProducerRecord
    producer.send(
      new ProducerRecord[String, String](
        "mobssprequestlog",
        null,
        new java.util.Date().getTime,
        null,
        "8|1578306500162|0||testreqid|0|200000|1207923508561002497|1207923656183726081|||1156310000|0|||0|||0|||||||||1|92|0||0|0|0|0||1409|1207928984820449280|0|92,3|2,3|1205,1208|1,2"
      ))
    producer.send(
      new ProducerRecord[String, String](
        "mobsspimpresslog",
        null,
        new java.util.Date().getTime,
        null,
        "10|1578306500168|0|0||testreqid|1|100|0|92|0|1207923508561002497|1207923656183726081|172.25.54.66|1156310000|0|||4||||||1409"
      ))


    // 11|1578540694158|0|0||afdbc02e-0f00-4d13-a9e2-3d93f36dfbd0|1|100|0|12|0|1214736094141042689|1214880077596995585|172.25.52.165|1000000000|0|meizu|Note9|4|39dd910ed3f1819b8181d076b441975e80bb09c5|4525d7c37c023e1d3a078605efe8c624|f6c9815c2763ac2d695b6e114ade0873294b6542|d97dd9b8c8fb9b359496a3d4532a8c9d|b645bf2c0729ffcf|1214110856003543066
    producer.close()
  }
}
