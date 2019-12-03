package com.flink.common.sink

import com.flink.common.bean.AdlogBean
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class HbaseReportSink extends SinkFunction[AdlogBean]{

  override def invoke(value: AdlogBean): Unit = {
    println("HbaseReportSink",value)

  }
}
