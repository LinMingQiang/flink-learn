package com.flink.learn.sink

import com.flink.learn.bean.AdlogBean
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class HbaseReportSink extends SinkFunction[AdlogBean]{

  override def invoke(value: AdlogBean): Unit = {
    println("HbaseReportSink",value)

  }
}
