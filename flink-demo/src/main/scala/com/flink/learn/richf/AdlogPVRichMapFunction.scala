package com.flink.learn.richf

import com.flink.learn.bean.{AdlogBean, StatisticalIndic}
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

class AdlogPVRichMapFunction extends RichMapFunction[AdlogBean,AdlogBean]{
  var sum : ValueState[StatisticalIndic] = _
  override def map(value: AdlogBean): AdlogBean = {
    val newsum = if(sum == null){
      value.pv
    }else{
      StatisticalIndic(sum.value().pv+value.pv.pv)
    }
    sum.update(newsum)
    value.pv = newsum
    value
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[StatisticalIndic](
        "statisticalIndic",
        classOf[(StatisticalIndic)],
        StatisticalIndic(0)))
  }
}
