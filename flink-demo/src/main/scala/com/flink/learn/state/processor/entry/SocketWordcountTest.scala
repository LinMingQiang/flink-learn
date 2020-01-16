package com.flink.learn.state.processor.entry

import java.util.Date

import com.flink.common.core.FlinkEvnBuilder
import com.flink.learn.bean.CaseClassUtil.Wordcount
import com.flink.learn.bean.WordCountPoJo
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
    val source = env.socketTextStream("localhost", 9877)
    source
      .map(x => {
        val s = new WordCountPoJo()
        s.w = x;s.c = 1L; s.timestamp = new Date().getTime
        s
      })
      .keyBy(_.w)
      .flatMap(new RichFlatMapFunction[WordCountPoJo, WordCountPoJo] {
        var lastState: ValueState[WordCountPoJo] = _
        override def flatMap(value: WordCountPoJo,
                             out: Collector[WordCountPoJo]): Unit = {
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
            val desc = new ValueStateDescriptor[WordCountPoJo](
              "wordcountState",
              createTypeInformation[WordCountPoJo])
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
