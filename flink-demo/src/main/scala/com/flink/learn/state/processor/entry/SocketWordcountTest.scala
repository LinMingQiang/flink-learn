package com.flink.learn.state.processor.entry

import java.util.Date

import com.flink.common.core.FlinkEvnBuilder
import com.flink.learn.bean.CaseClassUtil.Wordcount
import com.flink.learn.entry.cp
import com.flink.learn.param.PropertiesUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object SocketWordcountTest {

  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildStreamingEnv(PropertiesUtil.param, cp, 60000) // 1 min
    val source = env.socketTextStream("localhost", 9876)
    source
      .map(x => Wordcount(x, 1L, new Date().getTime))
      .keyBy(_.w)
      .flatMap(new RichFlatMapFunction[Wordcount, Wordcount] {
        var lastState: ValueState[Wordcount] = _
        override def flatMap(value: Wordcount,
                             out: Collector[Wordcount]): Unit = {
          var ls = lastState.value()
          if (ls == null) {
            ls = value
          } else {
            ls.c += value.c
          }
          lastState.update(ls)
          out.collect(ls)
        }
        override def open(parameters: Configuration): Unit = {
          try {
            val desc = new ValueStateDescriptor[Wordcount](
              "wordcountState",
              createTypeInformation[Wordcount])
            lastState = getRuntimeContext().getState(desc)
          } catch {
            case e: Throwable => e.printStackTrace()
          }
        }
      })
      .name("wordcountUID")
      .uid("wordcountUID")
      .print()



    env.execute("SocketWordcountTest")
  }
}
