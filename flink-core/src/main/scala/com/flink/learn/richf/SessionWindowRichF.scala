package com.flink.learn.richf

import java.lang

import org.apache.flink.streaming.api.scala.function.{
  ProcessWindowFunction,
  RichWindowFunction
}
import com.flink.learn.bean.CaseClassUtil.{SessionLogInfo, SessionWindowResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
class SessionWindowRichF
    extends RichWindowFunction[SessionLogInfo,
                               SessionWindowResult,
                               String,
                               TimeWindow] {

  var state: ValueState[SessionWindowResult] = null

  override def open(parameters: Configuration): Unit = {

    state = getRuntimeContext.getState(
      new ValueStateDescriptor[SessionWindowResult](
        "snapshot State",
        classOf[SessionWindowResult],
        null))

  }

  /**
   * 统计每个key - 窗口里面的数据。计算session 在间隔gap时间里有多少次访问，时间间隔是多少
   * @param key
   * @param window
   * @param input
   * @param out
   */
  override def apply(key: String,
                     window: TimeWindow,
                     input: Iterable[SessionLogInfo],
                     out: Collector[SessionWindowResult]): Unit = {
    val list = input.toList
    val internalTime = list.maxBy(_.timeStamp).timeStamp - list.minBy(_.timeStamp).timeStamp
    out.collect(SessionWindowResult(key, list.size, internalTime))
  }

}
