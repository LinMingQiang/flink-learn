package com.flink.learn.entry

import java.util.Date

import com.flink.common.core.FlinkEvnBuilder
import com.flink.learn.bean.CaseClassUtil.Wordcount
import com.flink.learn.param.PropertiesUtil
import com.flink.learn.time.MyTimestampsAndWatermarks
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
object FlinkJoinWatermarkTest {

  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildStreamingEnv(PropertiesUtil.param, cp, 60000) // 1 min
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 时间设为eventime
    env.getConfig.setAutoWatermarkInterval(5000L)
    intervalJoin(env)
//    val source = env.socketTextStream("localhost", 9876)
//    source
//      .map(x => Wordcount(x, 1L, new Date().getTime))
//      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(10000L))
//      .join(
//        env
//          .socketTextStream("localhost", 9877)
//          .map(x => Wordcount(x, 1L, new Date().getTime))
//          .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(10000L)))
//      .where(_.w)
//      .equalTo(_.w)
//      .window()
//      .print()

    env.execute("SocketWordcountTest")
  }

  /**
    * 双流join。 join不到的保留 10s 。之后过期
    * @param env
    */
  def intervalJoin(env: StreamExecutionEnvironment): Unit = {
    val click = env
      .socketTextStream("localhost", 9876)
      .map(x => Wordcount(x, 1L, new Date().getTime))
      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(10000L))
    val expose = env
      .socketTextStream("localhost", 9877)
      .map(x => Wordcount(x, 1L, new Date().getTime))
      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(10000L))
    // 点击去join 前 后 10s的曝光。 允许点击比曝光早和晚 10s。
    click
      .keyBy(_.w)
      .intervalJoin(expose.keyBy(_.w))
      .between(Time.milliseconds(-10000), Time.milliseconds(10000))
      .process(new ProcessJoinFunction[Wordcount, Wordcount, Wordcount]() {
        override def processElement(in1: Wordcount,
                                    in2: Wordcount,
                                    context: ProcessJoinFunction[
                                      Wordcount,
                                      Wordcount,
                                      Wordcount]#Context,
                                    collector: Collector[Wordcount]): Unit = {

          println(in1, in2)
        }
      })
      .print()

  }
}
