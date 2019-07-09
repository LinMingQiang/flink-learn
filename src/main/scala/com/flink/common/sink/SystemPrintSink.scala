package com.flink.common.sink

import com.flink.common.entry.LocalFlinkTest.WordCount
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class SystemPrintSink extends RichSinkFunction[WordCount]{
  override def invoke(value: WordCount): Unit = {
    println(value)
  }
}
