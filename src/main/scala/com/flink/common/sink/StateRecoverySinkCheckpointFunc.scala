package com.flink.common.sink

import com.flink.common.bean.AdlogBean
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ListBuffer

/**
  * @author LMQ
  * @desc 用于在checkpoint时，checkpoint恢复时候调用。此方法用于恢复checkpoint里面的state
  */
class StateRecoverySinkCheckpointFunc(threshold: Int = 10) extends SinkFunction[AdlogBean] with CheckpointedFunction{
  @transient
  private var checkPointedState: ListState[(AdlogBean)] = _
  private var checkPointedSBuffer = ListBuffer[(AdlogBean)]()
  private val bufferedElements = ListBuffer[(AdlogBean)]() //用于批量提交，如果写hbase的话，建议批量提交，例如一个分区数量达到100了，那就提交一次

  /**
    * @desc 在程序恢复checkpoint的时候调用
    * @param context
    */
  override def initializeState(context: FunctionInitializationContext): Unit ={
    println(">>> initializeState <<")
    val descriptor =new ListStateDescriptor[AdlogBean](
        "buffered-elements",
        classOf[AdlogBean])
    checkPointedState = context.getOperatorStateStore().getListState(descriptor);
    if (context.isRestored()) {
      var count = 0
      println(">>> initializeState recovery <<")
      val it = checkPointedState.get().iterator()
      while(it.hasNext){
        val adlog = it.next()
        bufferedElements.+=(adlog)
        count+=1
      }
      println(">>> initializeState recovery << "+ count)
    }
  }

  /**
    * @desc 在程序执行checkpoint的时候调用
    * @param context
    */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
   println(">>> snapshotState <<",checkPointedSBuffer.size)
    //执行顺序应该是，先这个，然后再
    checkPointedState.clear()//清除之前状态以装载新的状态数据
    for (element <- checkPointedSBuffer) {
      checkPointedState.add(element)
   }
    checkPointedSBuffer.clear()
  }

  /**
    * @desc 输出调用
    * @param value
    */
  override def invoke(value: AdlogBean): Unit = {
    println(value)
    bufferedElements += value
    if (bufferedElements.size == threshold) {

      //数量达到上限。
      //do something (write to hbase or hdfs ...)
      checkPointedSBuffer.++=(bufferedElements)
      bufferedElements.clear()
    }
  }


}
