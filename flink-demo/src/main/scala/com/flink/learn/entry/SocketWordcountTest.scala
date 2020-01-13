package com.flink.learn.entry

import java.util.Date

import com.flink.learn.bean.CaseClassUtil.Wordcount
import com.flink.learn.bean.{AdlogBean, StatisticalIndic}
import com.flink.common.core.FlinkEvnBuilder
import com.flink.learn.entry.LocalFlinkTest.cp
import com.flink.learn.param.PropertiesUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{
  ValueState,
  ValueStateDescriptor
}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SocketWordcountTest {
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildFlinkEnv(PropertiesUtil.param, cp, 60000) // 1 min
    env.setParallelism(1)
    val source = env.socketTextStream("localhost", 9876)
    source
      .map(x => Wordcount(x, 1L))
      .keyBy(x => x.w)
      .flatMap(new RichFlatMapFunction[Wordcount, (String, Long)] {
        var lastState: ValueState[Long] = _
        override def flatMap(value: Wordcount,
                             out: Collector[(String, Long)]): Unit = {

          lastState.update(t)
          out.collect((value._1, t))
        }
        override def open(parameters: Configuration): Unit = {
          import org.apache.flink.streaming.api.scala._
          val desc = new ValueStateDescriptor[Long](
            "StatisticalIndic",
            createTypeInformation[Long])
          // desc.enableTimeToLive(ttlConfig) // TTL
          desc.setQueryable("StatisticalIndic")
          lastState = getRuntimeContext().getState(desc)
        }
      })
      .name("StatisticalIndic")
      .uid("StatisticalIndic")
      .print()
    env.execute("SocketWordcountTest")
  }
}
