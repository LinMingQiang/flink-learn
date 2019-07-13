package com.flink.common.entry
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

import scala.collection.JavaConversions._
import java.util.Properties

import com.flink.common.bean.{AdlogBean, StatisticalIndic}
import com.flink.common.richf.{
  AdlogPVRichFlatMapFunction,
  AdlogPVRichMapFunction
}
import com.flink.common.sink.{
  HbaseReportSink,
  StateRecoverySinkCheckpointFunc,
  SystemPrintSink
}

object LocalFlinkTest {
  val cp = "file:///C:\\Users\\Master\\Desktop\\rocksdbcheckpoint"

  /**
    * @author LMQ
    * @version 1.8.0
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println("LocalFlinkTest ... ")
    val kafkasource = new FlinkKafkaConsumer08[(String, String)](
      TOPIC.split(",").toList,
      new TopicMessageDeserialize(),
      getKafkaParam)
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    val env = getFlinkEnv(cp)
    val result = env
      .addSource(kafkasource)
      .map { x =>
        val datas = x._2.split(",")
        val statdate = datas(0).substring(0, 10) //日期
        val hour = datas(0).substring(11, 13) //hour
        val plan = datas(25)
        if (plan.nonEmpty) {
          new AdlogBean(plan, statdate, hour, StatisticalIndic(1))
        } else null
      }
      .filter { x =>
        x != null && x.plan.equals("J4hUm12U0mI")
      }
      .keyBy(_.key) //按key分组，可以把key相同的发往同一个slot处理
      .flatMap(new AdlogPVRichFlatMapFunction) //通常都是用的flatmap，功能类似 (filter + map)

    //result.addSink(new StateRecoverySinkCheckpointFunc(10))
    //多个sink输出
    result.addSink(new SystemPrintSink)
    //result.addSink(new HbaseReportSink)

    env.execute("lmq-flink-demo") //程序名
  }

}
