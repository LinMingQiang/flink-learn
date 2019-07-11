package com.flink.common.entry

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

import scala.collection.JavaConversions._
import java.util.Properties

import com.flink.common.bean.{AdlogBean, StatisticalIndic}
import com.flink.common.richf.{AdlogPVRichFlatMapFunction, AdlogPVRichMapFunction, StateRecoveryRichFlatmapFunc}
import com.flink.common.sink.{StateRecoverySinkCheckpointFunc, SystemPrintSink}

object InitStateByCheckpoint {
  val cp="file:///C:\\Users\\Master\\Desktop\\init_checkpoint"
  def main(args: Array[String]): Unit = {
    println(cp)
    val pro = new Properties();
    pro.put("bootstrap.servers", BROKER);
    pro.put("zookeeper.connect", KAFKA_ZOOKEEPER);
    pro.put("group.id", "test")
    pro.put("auto.commit.enable", "true")//kafka 0.8-
    pro.put("enable.auto.commit", "true")//kafka 0.9+
    pro.put("auto.commit.interval.ms", "60000");
    val kafkasource = new FlinkKafkaConsumer08[(String, String)](TOPIC.split(",").toList, new TopicMessageDeserialize(), pro)
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    val env = getFlinkEnv(cp)
    env
      .addSource(kafkasource)
      .map { x =>
        val datas = x._2.split(",")
        val statdate = datas(0).substring(0, 10) //日期
      val hour = datas(0).substring(11, 13) //hour
      val plan = datas(25)
        if (plan.nonEmpty) {
          new AdlogBean(plan,statdate,hour,StatisticalIndic(1))
        } else null
      }
      .filter { x=> x != null && x.plan.equals("J4hUm12U0mI")}
      .keyBy(_.key) //按key分组，可以把key相同的发往同一个slot处理
      .flatMap(new AdlogPVRichFlatMapFunction)//通常都是用的flatmap，功能类似 (filter + map)
      //.print
      .addSink(new SystemPrintSink)


    env.execute("InitStateByCheckpoint")
  }

}
