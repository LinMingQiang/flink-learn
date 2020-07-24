package com.flink.pro.rich

import com.flink.common.kafka.KafkaManager
import com.flink.pro.common.CaseClassUtil.SourceLogData
import com.flink.pro.common.StreamPropertiesUtil
import com.flink.pro.common.StreamPropertiesUtil._
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.apache.flink.util.Collector
import org.slf4j.LoggerFactory

class AnalysisLogData
    extends RichFlatMapFunction[KafkaManager.KafkaMessge, SourceLogData] {
  var parame: ParameterTool = _
  val _log = LoggerFactory.getLogger(this.getClass)
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
    * 数据简单转化，之后按照topic+reqid去重
    * @param in
    * @param out
    */
  override def flatMap(in: KafkaManager.KafkaMessge,
                       out: Collector[SourceLogData]): Unit = {
    try {
      val arr = in.msg.split("\\|", -1)
      in.topic match {
        case REQUEST_TOPIC =>
          if (arr.size >= REQ_BID_LOG_SIZE) {
            out.collect(SourceLogData(REQUEST_TOPIC, arr(1), arr))
          }
        //  else println(s"errsize : $REQUEST_TOPIC : ${in.msg}")
        case IMPRESS_TOPIC =>
          if (arr.size >= IMPRESS_CLICK_LOG_SIZE) {
            out.collect(SourceLogData(IMPRESS_TOPIC, arr(1), arr))
          }
        //  else println(s"errsize： $IMPRESS_TOPIC : ${in.msg}")
        case CLICK_TOPIC =>
          if (arr.size >= IMPRESS_CLICK_LOG_SIZE) {
            out.collect(SourceLogData(CLICK_TOPIC, arr(1), arr))
          }
        // else println(s"errsize ：$CLICK_TOPIC : ${in.msg}")
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }

  }
}
