package com.flink.common.richf

import com.flink.common.bean.{AdlogBean, StatisticalIndic}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class AdlogPVRichFlatMapFunction extends RichFlatMapFunction[AdlogBean, AdlogBean] {
  var lastState: ValueState[StatisticalIndic] = _

  override def flatMap(value: AdlogBean, out: Collector[AdlogBean]): Unit = {

    val ls = lastState.value()
    val news = StatisticalIndic(ls.pv + value.pv.pv)
    lastState.update(news)
    value.pv=news
    out.collect(value)
  }

  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[(StatisticalIndic)](
      "StatisticalIndic", classOf[(StatisticalIndic)], StatisticalIndic(0))
    lastState = getRuntimeContext().getState(desc)
  }

}
