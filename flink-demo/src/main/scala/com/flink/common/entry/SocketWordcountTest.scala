package com.flink.common.entry

import com.flink.common.bean.CaseClassUtil.Wordcount
import com.flink.common.bean.{AdlogBean, StatisticalIndic}
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.entry.LocalFlinkTest.cp
import com.flink.common.param.PropertiesUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SocketWordcountTest {
  val cp = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildFlinkEnv(PropertiesUtil.param, cp, 60000) // 1 min
    val source = env.socketTextStream("localhost", 9876)
    source
      .map(x => Wordcount(x, 1L))
      .keyBy("w")
      .flatMap(new RichFlatMapFunction[Wordcount, Wordcount] {
        var lastState: ValueState[java.lang.Long] = _
        override def flatMap(value: Wordcount,
                             out: Collector[Wordcount]): Unit = {
          val ls = lastState.value()
          val lastv = if (ls == null) 0L else ls
          val news = lastv.toString.toLong + value.c
          lastState.update(news)
          value.c = news
          out.collect(value)
        }
        override def open(parameters: Configuration): Unit = {
          import org.apache.flink.streaming.api.scala._
          try{
            val desc = new ValueStateDescriptor[java.lang.Long]("StatisticalIndic",
              createTypeInformation[java.lang.Long])
            // desc.enableTimeToLive(ttlConfig) // TTL
            desc.setQueryable("StatisticalIndic")
            lastState = getRuntimeContext().getState(desc)
          }
          catch {
            case e: Throwable => e.printStackTrace()
          }
        }
      })
      .name("StatisticalIndic")
      .uid("StatisticalIndic")
      .print()
    env.execute("SocketWordcountTest")
  }
}
