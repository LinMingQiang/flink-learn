package com.flink.learn.state.processor.entry

import com.flink.learn.bean.CaseClassUtil.Wordcount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.streaming.api.scala.createTypeInformation

class AccountKeyedStateBootstrapFunction()
    extends KeyedStateBootstrapFunction[Tuple, Wordcount] {
  var lastState: ValueState[Wordcount] = _
  override def open(parameters: Configuration): Unit = {
    println(">>>>>>>>>>>>")
    val desc = new ValueStateDescriptor[Wordcount](
      "wordcountState",
      createTypeInformation[Wordcount])
    lastState = getRuntimeContext().getState(desc)
  }

  override def processElement(
      value: Wordcount,
      ctx: KeyedStateBootstrapFunction[Tuple, Wordcount]#Context): Unit = {

    value.c = 1000L
    if(lastState == null) println(">>>>>>")
    lastState.update(value)

  }
}
