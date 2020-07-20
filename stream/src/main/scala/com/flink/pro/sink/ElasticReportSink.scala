package com.flink.pro.sink

import java.util.Date

import com.flink.pro.common.CaseClassUtil.ReportInfo
import com.flink.common.dbutil.ElasticsearchHandler
import com.flink.pro.common.StreamPropertiesUtil
import com.flink.pro.func.ElasticDocAssembleUtil
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.elasticsearch.action.bulk.{BulkProcessor, BulkRequestBuilder}
import org.elasticsearch.client.transport.TransportClient
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
class ElasticReportSink(size: Int, interval: Long)
    extends RichSinkFunction[ReportInfo]
    with CheckpointedFunction {
  val _log = LoggerFactory.getLogger(this.getClass)
  var client: TransportClient = null
  private var checkpointedState: ListState[ReportInfo] = _ // checkpoint state
  private val bufferedElements = mutable.HashMap[String, ReportInfo]() // buffer List
  var nextTime = 0L // 下一次提交时间
  var taskIndex = 0 // 当前slot的id
  /**
    * 初始化配置
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    _log.info("ElasticReportSink : open ")
    taskIndex = this.getRuntimeContext
      .asInstanceOf[StreamingRuntimeContext]
      .getIndexOfThisSubtask
    val parame = getRuntimeContext()
      .getExecutionConfig()
      .getGlobalJobParameters()
      .asInstanceOf[ParameterTool]
    StreamPropertiesUtil.init(parame)
    client = ElasticsearchHandler.getEsClient(
      StreamPropertiesUtil.ES_HOSTS,
      StreamPropertiesUtil.ES_CLUSTERNAME)
  }

  /**
    * 批量提交至es
    * @param value
    */
  override def invoke(value: ReportInfo): Unit = {
    try {
      bufferedElements.put(value.keybyKey, value) // 每次都保存最新的
      if (new Date().getTime > nextTime || bufferedElements.size > size) { // 每个一分钟提交一次
        nextTime = new Date().getTime + 1000 * interval
        val bulk = client.prepareBulk()
        bufferedElements.foreach(x => {
          val index = ElasticDocAssembleUtil.getUpsertIndex(x._2)
          bulk.add(index)
        })
        commitBulk(bulk)
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  /**
    * 为了防止，数据后面不更新了，导致这批缓存不更新，在做快照的时候提交一次
    * @param functionSnapshotContext
    */
  override def snapshotState(
      functionSnapshotContext: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    val checkSize = bufferedElements.size
    val bulk = client.prepareBulk()
    for (element <- bufferedElements) {
      checkpointedState.add(element._2)
      val index = ElasticDocAssembleUtil.getUpsertIndex(element._2)
      bulk.add(index)
    }
    commitBulk(bulk)
    checkpointedState.clear()
    _log.info(s"${taskIndex}> suesse snapshotState : $checkSize ")
  }

  /**
    * 初始化恢复
    * @param functionInitializationContext
    */
  override def initializeState(
      functionInitializationContext: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[ReportInfo](
      "buffered-elements",
      TypeInformation.of(new TypeHint[ReportInfo]() {}))
    checkpointedState = functionInitializationContext.getOperatorStateStore
      .getListState(descriptor)
    if (functionInitializationContext.isRestored) {
      _log.info(s"${taskIndex}> --- initializeState ---")
      for (element <- checkpointedState.get()) {
        bufferedElements += (element.keybyKey -> element)
        _log.info(s"restore : $element")
      }
    }
  }

  /**
    * 提交bulk
    * @param bulk
    */
  def commitBulk(bulk: BulkRequestBuilder): Unit = {
    if (bulk.numberOfActions() > 0) {
      if (bulk.get().hasFailures) { // 如果失败了不要clear
        _log.error(s"${taskIndex}>  ElasticReportSink : bulk fail retry ")
        _log.error(
          s"${taskIndex}>  Retry is Fail ? -> ${bulk.get().hasFailures}")
      } else bufferedElements.clear()
    }
  }
}
