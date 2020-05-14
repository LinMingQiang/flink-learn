package com.flink.learn.sink

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
class OperatorStateBufferingSink(threshold: Int = 3)
    extends SinkFunction[(String, Int)]
    with CheckpointedFunction {

  @transient
  private var checkpointedState
    : ListState[(String, Int)] = _ // checkpoint state
  private val bufferedElements = ListBuffer[(String, Int)]() // buffer List
  /**
    * 可做批量写出数据，但是有问题就是如果没有数据，那这个缓冲数据会一直不写出来。但是可以在snapshotState的时候提交，
    * @param value
    */
  override def invoke(value: (String, Int)): Unit = {
    bufferedElements += value
    if (bufferedElements.size >= threshold) {
      for (element <- bufferedElements) {
        // send it to the sink
      }
      bufferedElements.clear()
    }
  }

  /**
    * 要实现，否则报Rocksdb not find libary
    * @param functionSnapshotContext
    */
  override def snapshotState(
      functionSnapshotContext: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    println("start snapshotState")
    for (element <- bufferedElements) {
      checkpointedState.add(element)
      println(element)
    }
    println("end snapshotState")
  }

  /**
    *
    * @param context
    */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[(String, Int)](
      "buffered-elements",
      TypeInformation.of(new TypeHint[(String, Int)]() {})
    )
    checkpointedState = context.getOperatorStateStore.getListState(descriptor)
    if (context.isRestored) {
      println("--- initializeState ---")
      for (element <- checkpointedState.get()) {
        bufferedElements += element
        println("operator state : ", element)
      }
    }
  }
}
