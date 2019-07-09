package com.flink.common.entry
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

import scala.collection.JavaConversions._
import java.util.Properties

import scala.beans.BeanProperty

object LocalFlinkTest {
  /**
    * @author LMQ
    * @version 1.8.0
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println(">>>>>>>>>>>>>")
    val pro = new Properties();
    pro.put("bootstrap.servers", BROKER);
    pro.put("zookeeper.connect", KAFKA_ZOOKEEPER);
    pro.put("group.id", "test");
    pro.put("auto.commit.enable", "true")
    pro.put("auto.commit.interval.ms", "60000");
    val kafkasource = new FlinkKafkaConsumer08[(String, String)](TOPIC.split(",").toList, new TopicMessageDeserialize(), pro)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    val env = getFlinkEnv()
    env
      .addSource(kafkasource)
      .map { x =>
        val datas = x._2.split(",")
        val statdate = datas(0).substring(0, 10) //日期
        val hour = datas(0).substring(11, 13) //hour
        val plan = datas(25)
        if (plan.nonEmpty) {
          new WordCount(plan,statdate,hour,1)
        } else null
      }
      .filter { _ != null }
      .keyBy(_.key) //按key分组，可以把key相同的发往同一个slot处理
      .sum("pv")
      //.map(new AdlogPVRichMapFunction)
      .print
      .setParallelism(6)
      //.sum("pv")
      //.addSink(new SystemPrintSink)

    env.execute()
  }
  case class WordCount(
      var plan: String, 
      var startdate:String,
      var hour:String,
      var pv: Int) {
    @BeanProperty
    val key = s"${startdate},${plan},${hour}"
    override def toString() = {
      (key,pv).toString()
    }
  }
}