package com.flink.learn.stateprocessor

import java.util.Date

import com.flink.common.core.FlinkLearnPropertiesUtil.{FLINK_DEMO_CHECKPOINT_PATH, param}
import com.flink.common.core.{EnvironmentalKey, FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.learn.bean.WordCountPoJo
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

object SocketJavaPoJoWordcountTest {
// nc -l 9877
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
      "SocketJavaPoJoWordcountTest")
    val env = FlinkEvnBuilder.buildStreamingEnv(param,
      FLINK_DEMO_CHECKPOINT_PATH,
      10000)
    val source = env.socketTextStream("localhost", 9877)
    source
      .map(x => {
        val s = new WordCountPoJo()
        s.word = x;s.count = 1L; s.timestamp = new Date().getTime
        s
      })
      .keyBy(x => new org.apache.flink.api.java.tuple.Tuple2(x.word, x.word))
      .flatMap(new RichFlatMapFunction[WordCountPoJo, WordCountPoJo] {
        var lastState: ValueState[WordCountPoJo] = _
        override def flatMap(value: WordCountPoJo,
                             out: Collector[WordCountPoJo]): Unit = {
          var ls = lastState.value()
          if (ls == null) {
            ls = value
          } else {
            ls.count += value.count
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
