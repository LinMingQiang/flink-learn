package com.flink.pro.rich

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RuntimeContext}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.slf4j.LoggerFactory

trait RichFlatMapFuncTrait[IN, OUT] extends RichFlatMapFunction[IN, OUT] {
  var parame: ParameterTool = _
  val _log = LoggerFactory.getLogger(this.getClass)
  var taskIndex = 0L

  /**
    *
    * @param parame
    */
  def initParam(parame: ParameterTool)

  def initState(rt: RuntimeContext) = {}
  /**
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    taskIndex = this.getRuntimeContext
      .asInstanceOf[StreamingRuntimeContext]
      .getIndexOfThisSubtask
    parame = getRuntimeContext()
      .getExecutionConfig()
      .getGlobalJobParameters()
      .asInstanceOf[ParameterTool]
    initParam(parame)
    initState(getRuntimeContext)
    _log.info(s"${taskIndex}> open :  ${parame.toMap}")
  }
}
