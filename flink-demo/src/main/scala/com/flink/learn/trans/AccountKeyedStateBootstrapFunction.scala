package com.flink.learn.trans

import java.util.Date

import com.flink.learn.bean.CaseClassUtil.WordCountScalaPoJo
import com.flink.learn.bean.WordCountPoJo
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.streaming.api.scala.createTypeInformation

class AccountKeyedStateBootstrapFunction()
    extends KeyedStateBootstrapFunction[Tuple, WordCountScalaPoJo] {
  var lastState: ValueState[WordCountScalaPoJo] = _
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[WordCountScalaPoJo](
      "wordcountState",
      createTypeInformation[WordCountScalaPoJo])
    lastState = getRuntimeContext().getState(desc)
  }

  override def processElement(
      value: WordCountScalaPoJo,
      ctx: KeyedStateBootstrapFunction[Tuple, WordCountScalaPoJo]#Context)
    : Unit = {

    value.count = 1L
    lastState.update(value)

  }
}
