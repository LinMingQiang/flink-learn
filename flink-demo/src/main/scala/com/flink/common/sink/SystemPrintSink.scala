package com.flink.common.sink

import com.flink.common.bean.{AdlogBean, StatisticalIndic}
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}

class SystemPrintSink extends SinkFunction[AdlogBean] {
  override def invoke(value: AdlogBean): Unit = {
    println("SystemPrintSink",value)
  }
}
