package com.flink.learn.reader

import java.util.function.Consumer

import com.flink.learn.bean.{WordCountGroupByKey, WordCountPoJo}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.state.api.functions.KeyedStateReaderFunction
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._
class WordCountJavaPojoOpearateKeyreader(stateName: String)
    extends KeyedStateReaderFunction[WordCountGroupByKey, WordCountPoJo] {
  var lastState: ListState[WordCountPoJo] = _
  override def open(parameters: Configuration): Unit = {
    val desc = new ListStateDescriptor[WordCountPoJo](
      stateName,
      createTypeInformation[WordCountPoJo])
    lastState = getRuntimeContext().getListState(desc)
  }

  override def readKey(key: WordCountGroupByKey,
                       context: KeyedStateReaderFunction.Context,
                       collector: Collector[WordCountPoJo]): Unit = {
   lastState.get().asScala.foreach(collector.collect(_))
  }
}
