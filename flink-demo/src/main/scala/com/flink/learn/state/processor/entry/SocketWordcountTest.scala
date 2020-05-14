package com.flink.learn.state.processor.entry

import java.util.Date

import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.common.core.FlinkLearnPropertiesUtil.{FLINK_DEMO_CHECKPOINT_PATH, param}
import com.flink.learn.bean.WordCountPoJo
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object SocketWordcountTest {

  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
      "KafkaWordCountTest")
    val env = FlinkEvnBuilder.buildStreamingEnv(param,
      FLINK_DEMO_CHECKPOINT_PATH,
      10000) // 1 min
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
