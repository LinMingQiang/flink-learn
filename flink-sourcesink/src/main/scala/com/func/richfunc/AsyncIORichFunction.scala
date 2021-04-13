package com.func.richfunc

import com.flink.common.kafka.KafkaManager.KafkaMessge
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}

class AsyncIORichFunction
    extends RichAsyncFunction[KafkaMessge, Tuple2[KafkaMessge, KafkaMessge]] {
  override def asyncInvoke(
      input: KafkaMessge,
      resultFuture: ResultFuture[(KafkaMessge, KafkaMessge)]): Unit = {

  }
}
