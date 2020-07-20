package com.flink.pro.rich

import com.flink.pro.common.CaseClassUtil.{ReportInfo, SourceLogData}
import com.flink.pro.common.StreamPropertiesUtil
import com.flink.pro.func.DistinctReqidDataFormatUtil
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class DataFormatReportInfoRichfm
    extends RichFlatMapFunction[SourceLogData, ReportInfo] {
  val _log = LoggerFactory.getLogger(this.getClass)
  var parame: ParameterTool = _
  var taskIndex = 0L

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
    StreamPropertiesUtil.init(parame)
    _log.info(s"${taskIndex}> open :  ${parame.toMap}")
  }

  /**
    *
    * @param value
    * @param out
    */
  override def flatMap(value: SourceLogData,
                       out: Collector[ReportInfo]): Unit = {
    DistinctReqidDataFormatUtil.transAndFormatLog(value, out)
  }
}
