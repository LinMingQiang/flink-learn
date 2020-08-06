package com.flink.learn.richf

import com.flink.learn.bean.CaseClassUtil.{SessionLogInfo, Wordcount}
import com.flink.learn.bean.CountWithTimestamp
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class SessiontProcessFunction
    extends KeyedProcessFunction[String, SessionLogInfo, (String, Long)] {

  /** The state that is maintained by this process function */
  // 保留某个key的统计状态
  lazy val state: ValueState[(Long, Long)] = getRuntimeContext
    .getState(
      new ValueStateDescriptor[(Long, Long)]("myState", classOf[(Long, Long)]))
  var lastTimer = 0L
  override def processElement(
      value: SessionLogInfo,
      ctx: KeyedProcessFunction[String, SessionLogInfo, (String, Long)]#Context,
      out: Collector[(String, Long)]): Unit = {
    try {
      if (state.value == null) {
        state.update((value.timeStamp, value.timeStamp))
      } else {
        if (state.value._2 < value.timeStamp) {
          state.update((state.value._1, value.timeStamp))
          // ctx.timerService.deleteEventTimeTimer(lastTimer)
        }
      }
      lastTimer = state.value._2 + 20000
      ctx.timerService.registerEventTimeTimer(lastTimer)
      // 给当前key注册一个定时器。 每次这个key有更新就重新注册一个定时器，
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
                       ctx: KeyedProcessFunction[String,
                                                 SessionLogInfo,
                                                 (String, Long)]#OnTimerContext,
                       out: Collector[(String, Long)]): Unit = {
    val key = ctx.getCurrentKey
    println("onTimer ： ", key, timestamp, state.value)
    if (state.value._2 - state.value._1 > 0)
      out.collect((key, state.value._2 - state.value._1))
    state.clear()
  }
}
