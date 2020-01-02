package com.flink.learn.sink

import com.flink.learn.bean.AdlogBean
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

import scala.collection.mutable.ListBuffer

/**
  * @author LMQ
  * @desc 用于在checkpoint时，checkpoint恢复时候调用。此方法用于恢复checkpoint里面的state
  */
class StateRecoverySinkCheckpointFunc(threshold: Int)
    extends SinkFunction[AdlogBean]
    with CheckpointedFunction {
  @transient
  private var checkPointedState: ListState[(AdlogBean)] = _
  private val bufferedElements = ListBuffer[(AdlogBean)]() //用于批量提交，如果写hbase的话，建议批量提交，例如一个分区数量达到100了，那就提交一次
  /**
    *  在程序恢复checkpoint的时候调用。如果当前这个slot失败了也会调用这个来恢复
    * @param context
    */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[AdlogBean]("buffered-elements",
                                                        classOf[AdlogBean])
    checkPointedState = context.getOperatorStateStore().getListState(descriptor);
    if (context.isRestored()) { //从checkpoint中恢复
      var count = 0
      val it = checkPointedState.get().iterator() //获取恢复的state
      while (it.hasNext) {
        val adlog = it.next()
        bufferedElements.+=(adlog) //将上次的state恢复到内存中
        count += 1
      }
      println("initializeState recovery :",
              count,
              bufferedElements.take(1))
    }
  }

  /**
    * @desc 在程序执行checkpoint的时候调用,把上ck到当前ck的状态存储下来
    * @param context
    */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    println("check : ", bufferedElements.size, context.getCheckpointId)
    checkPointedState.clear() //清除之前状态以装载新的状态数据
    for (element <- bufferedElements) { //把当前的清空
      checkPointedState.add(element)
    }
  }

  /**
    * @desc 输出调用
    * @param value
    */
  override def invoke(value: AdlogBean): Unit = {
    bufferedElements += value
    //数量达到上限。例如每1000 写一次hbase
    if (bufferedElements.size >= threshold) {
      println("invoke :", bufferedElements.size)
      //do something (write to hbase or hdfs ...)
      //bufferedElements.foreach(println)
      //然后清空数据
      bufferedElements.clear() //当前批次数据处理完成
    }
  }

}
