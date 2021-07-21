package com.flink.scala.test

import java.util.Date

import com.flink.learn.test.common.FlinkStreamCommonSuit

class FlinkStreamJoinTest extends FlinkStreamCommonSuit {

//  test("window join") {
//    env.getConfig.setAutoWatermarkInterval(5000L)
//    val source1 = env
//      .addSource(kafkaSource("test", "localhost:9092"))
//      .map(x => Wordcount(x.msg, 11L, new Date().getTime))
//      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(10000L))
//    val source2 = env
//      .addSource(kafkaSource("test2", "localhost:9092"))
//      .map(x => Wordcount(x.msg, 22L, new Date().getTime))
//      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(10000L))
//
//
//    source1
//      .join(source2)
//      .where(_.word)
//      .equalTo(_.word)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) // 窗口太小没数据
//      // .evictor(TimeEvictor.of(Time.seconds(2))) // 只保留 窗口内最近2s的数据做计算
//      // .trigger(EventTimeTrigger.create()) // 以eventtime 时间触发窗口，当wartermark 》 window endtime 触发
//      // .sum(1) // 10s窗口的数据
//      .apply(new FlatJoinFunction[Wordcount, Wordcount, Wordcount] {
//        override def join(first: Wordcount,
//                          second: Wordcount,
//                          out: Collector[Wordcount]): Unit = {
//          println(first, second)
//          out.collect(
//            Wordcount(first.word, first.count + second.count, first.timestamp))
//        }
//      })
//      .print()
//
//    env.execute("")
//  }

  /**
    * 不同于join（基于窗口的join，不能跨窗口）。
    * intervalJoin是基于时间边界的，主表join 某个时间边界内的数据
    * 必须设置     env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    */
//  test("intervalJoin") {
//    val click =
//      env
//        .addSource(kafkaSource("test", "localhost:9092"))
//        .map(x => Wordcount(x.msg, 1L, new Date().getTime))
//        .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(100000L))
//    val expose =
//      env
//        .addSource(kafkaSource("test2", "localhost:9092"))
//        .map(x => Wordcount(x.msg, 1L, new Date().getTime))
//        .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks(100000L))
//    // 点击去join 前 后 10s的曝光。 允许点击比曝光早和晚 10s。
//    click
//      .keyBy(_.word)
//      .intervalJoin(expose.keyBy(_.word))
//      .between(Time.milliseconds(-100000), Time.milliseconds(100000))
//      .process(new ProcessJoinFunction[Wordcount, Wordcount, Wordcount]() {
//        override def processElement(in1: Wordcount,
//                                    in2: Wordcount,
//                                    context: ProcessJoinFunction[
//                                      Wordcount,
//                                      Wordcount,
//                                      Wordcount]#Context,
//                                    collector: Collector[Wordcount]): Unit = {
//
//          println(in1, in2)
//        }
//      })
//      .print()
//    env.execute("intervalJoin")
//  }
}
