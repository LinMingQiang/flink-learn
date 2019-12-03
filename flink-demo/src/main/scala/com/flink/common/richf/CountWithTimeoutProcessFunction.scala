package com.flink.common.richf

import com.flink.common.bean.CountWithTimestamp
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class CountWithTimeoutProcessFunction
    extends KeyedProcessFunction[Tuple, (String, String), (String, Long)] {

  /** The state that is maintained by this process function */
  // 保留某个key的统计状态
  lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
    .getState(
      new ValueStateDescriptor[CountWithTimestamp]("myState",
                                                   classOf[CountWithTimestamp]))

  override def processElement(value: (String, String),
                              ctx: KeyedProcessFunction[Tuple,
                                                        (String, String),
                                                        (String, Long)]#Context,
                              out: Collector[(String, Long)]): Unit = {
    try {
      /* env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime); 需要设置这个，否则  ctx.timestamp = null 报错
       * <p>This might be {@code null}, for example if the time characteristic of your program
       * is set to {@link org.apache.flink.streaming.api.TimeCharacteristic#ProcessingTime}.
       */
      val current: CountWithTimestamp = state.value match {
        case null =>
          CountWithTimestamp(value._1, 1, ctx.timestamp)
        case CountWithTimestamp(key, count, lastModified) =>
          CountWithTimestamp(key, count + 1, ctx.timestamp)
        case _ => println("null "); null
      }
      // write the state back
      state.update(current)
      // schedule the next timer 60 seconds from the current event time// 给当前key注册一个定时器。
      ctx.timerService.registerProcessingTimeTimer(current.lastModified + 60000)
      // ctx.timerService.registerEventTimeTimer(current.lastModified + 60000)
    } catch {
      case t: Throwable => t.printStackTrace()
    }

  }

  /**
    * 定时启动。启动时间由processElement 中设定
    * @param timestamp
    * @param ctx
    * @param out
    */
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Tuple,
                                                 (String, String),
                                                 (String, Long)]#OnTimerContext,
                       out: Collector[(String, Long)]): Unit = {
    val key = ctx.getCurrentKey
    println("onTimer ： ", key, timestamp, state.value)
    state.value match {
      case CountWithTimestamp(key, count, lastModified)
          if (timestamp == lastModified + 60000) =>
        out.collect((key, count))
      case _ =>
    }
  }
}
