package com.flink.learn.trans

import com.flink.learn.bean.CaseClassUtil._
import com.flink.learn.bean.WordCountGroupByKey
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction
import org.apache.flink.streaming.api.scala.createTypeInformation

class AccountPojoKeyedStateBootstrapFunction()
    extends KeyedStateBootstrapFunction[WordCountGroupByKey, TransWordCount] {

  /**
    *
    */
  var lastState: ValueState[Wordcount] = _
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor(
      "wordcountState",
      createTypeInformation[Wordcount]
    )
    lastState = getRuntimeContext().getState(desc)
  }

  override def processElement(
      v: TransWordCount,
      ctx: KeyedStateBootstrapFunction[WordCountGroupByKey, TransWordCount]#Context): Unit = {

    lastState.update(Wordcount(v.word, 1000L, v.timestamp))

  }
}
