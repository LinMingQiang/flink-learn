package com.flink.common.richf

import com.flink.common.entry.LocalFlinkTest.WordCount
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

class AdlogPVRichMapFunction extends RichMapFunction[WordCount,WordCount]{
  var sum : ValueState[Int] = _
  override def map(value: WordCount): WordCount = {
    val newsum = if(sum == null){
      value.pv
    }else{
      sum.value()+value.pv
    }
    sum.update(newsum)
    value.pv = newsum
    value
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Int)]("average", classOf[(Int)]))
  }
}
