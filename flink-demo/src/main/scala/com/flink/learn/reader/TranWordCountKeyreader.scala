package com.flink.learn.reader

import com.flink.learn.bean.CaseClassUtil.TransWordCount
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

class TranWordCountKeyreader(stateName: String)
    extends KeyedStateReaderFunction[String, TransWordCount] {
  var lastState: ValueState[TransWordCount] = _

  /**
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    val desc = new ValueStateDescriptor[TransWordCount](
      stateName,
      createTypeInformation[TransWordCount])
    lastState = getRuntimeContext().getState(desc)
  }

  /**
    *
    * @param key
    * @param context
    * @param collector
    */
  override def readKey(key: String,
                       context: KeyedStateReaderFunction.Context,
                       collector: Collector[TransWordCount]): Unit = {
    val v = lastState.value()
    val r = TransWordCount()
    r.word = v.word
    r.count = v.count
    r.timestamp = v.timestamp
    collector.collect(r)
  }
}
