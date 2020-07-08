package com.flink.commom.scala.streamsink

import org.apache.flink.streaming.api.functions.sink.SinkFunction

class SystemPrintSink[T] extends SinkFunction[T] {
  override def invoke(value: T): Unit = {
    println("SystemPrintSink: ", value)
  }
}
