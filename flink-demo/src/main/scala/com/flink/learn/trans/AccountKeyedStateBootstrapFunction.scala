package com.flink.learn.trans

import java.util.Date

import com.flink.learn.bean.CaseClassUtil.TransWordCount
import com.flink.learn.bean.TranWordCountPoJo
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.streaming.api.scala.createTypeInformation

class AccountKeyedStateBootstrapFunction()
    extends KeyedStateBootstrapFunction[Tuple, TranWordCountPoJo] {
  var lastState: ValueState[TranWordCountPoJo] = _
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[TranWordCountPoJo](
      "wordcountState",
      createTypeInformation[TranWordCountPoJo])
    lastState = getRuntimeContext().getState(desc)
  }

  override def processElement(
      v: TranWordCountPoJo,
      ctx: KeyedStateBootstrapFunction[Tuple, TranWordCountPoJo]#Context): Unit = {
//    val w = new TranWordCountPoJo()
//    w.w = v.word
//    w.c =v v.count;
//    w.timestamp = v.timestamp
    v.count = 10L
    lastState.update(v)

  }
}
