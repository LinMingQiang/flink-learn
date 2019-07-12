package com.flink.common.richf

import com.flink.common.bean.{AdlogBean, StatisticalIndic}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class AdlogPVRichFlatMapFunction
    extends RichFlatMapFunction[AdlogBean, AdlogBean] {
  var lastState: ValueState[StatisticalIndic] = _

  /**
    * @author LMQ
    * @desc 每次一套
    * @param value
    * @param out
    */
  override def flatMap(value: AdlogBean, out: Collector[AdlogBean]): Unit = {
    val ls = lastState.value()
    val news = StatisticalIndic(ls.pv + value.pv.pv)
    lastState.update(news)
    value.pv = news
    out.collect(value)
  }

  /**
    * @author LMQ
    * @desc 当首次打开此operator的时候调用，拿到 此key的句柄
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[(StatisticalIndic)](
      "StatisticalIndic",
      classOf[(StatisticalIndic)],
      StatisticalIndic(0))
    //desc.setQueryable("StatisticalIndic")
    lastState = getRuntimeContext().getState(desc)
  }

}
