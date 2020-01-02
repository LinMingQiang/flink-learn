package com.flink.learn.sink

import com.flink.learn.bean.{AdlogBean, StatisticalIndic}
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}

class SystemPrintSink extends SinkFunction[AdlogBean] {
  override def invoke(value: AdlogBean): Unit = {
    println("SystemPrintSink",value)
  }
}
