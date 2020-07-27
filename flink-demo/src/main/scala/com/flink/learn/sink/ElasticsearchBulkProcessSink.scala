package com.flink.learn.sink

import java.util.Date

import com.flink.common.core.FlinkLearnPropertiesUtil
import com.flink.common.dbutil.ElasticsearchHandler
import com.flink.learn.bean.CaseClassUtil.ReportInfo
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.transport.TransportClient

import scala.collection.JavaConversions._
import scala.collection.mutable

class ElasticsearchBulkProcessSink(size: Int, interval: Long)
    extends RichSinkFunction[ReportInfo]
    with CheckpointedFunction {
  var client: TransportClient = null
  private var checkpointedState: ListState[ReportInfo] = _ // checkpoint state
  private val bufferedElements = mutable.HashMap[String, ReportInfo]() // buffer List
  var nextTime = 0L
  // 用这个有一个缺点，就是提交的量有点大，而且数据是不是顺序提交的呢，会不会更新错乱？
  var bulk: BulkProcessor = null // 使用es自动提交功能，当没有数据的时候也能提交
  /**
    *
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    println("ElasticReportSink : open ")
    val parame = getRuntimeContext()
      .getExecutionConfig()
      .getGlobalJobParameters()
      .asInstanceOf[ParameterTool]
    FlinkLearnPropertiesUtil.init(parame)
    client = ElasticsearchHandler.getEsClient(FlinkLearnPropertiesUtil.ES_HOSTS,
      FlinkLearnPropertiesUtil.ES_CLUSTERNAME)
    bulk = ElasticsearchHandler.bulkProcessBuild(100, 10, 3)(client)

  }
  // (day, hour, buyerId, appid, adslot_id, policyId)
  /**
    *
    * @param value
    */
  override def invoke(value: ReportInfo): Unit = {
    try {
      bufferedElements.put(value.keybyKey, value)
      val index = new IndexRequest()// ElasticDocAssembleUtil.getUpsertIndex(value)
      println(value)
      bulk.add(index)
      if (new Date().getTime > nextTime || bufferedElements.size > size) { // 每个一分钟提交一次
        nextTime = new Date().getTime + 1000 * interval
        bulk.flush()
        bufferedElements.clear()
        println(" bulk commit success ")
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  /**
    * 快照
    */
  override def snapshotState(
      functionSnapshotContext: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    println("start snapshotState", bufferedElements.size)
    for (element <- bufferedElements) {
      checkpointedState.add(element._2)
    }
    println("end snapshotState")
  }

  /**
    * 初始化恢复
    * @param functionInitializationContext
    */
  override def initializeState(
      functionInitializationContext: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[ReportInfo](
      "buffered-elements",
      TypeInformation.of(new TypeHint[ReportInfo]() {})
    )
    checkpointedState = functionInitializationContext.getOperatorStateStore
      .getListState(descriptor)
    if (functionInitializationContext.isRestored) {
      println("--- initializeState ---")
      for (element <- checkpointedState.get()) {
        bufferedElements += (element.keybyKey -> element)
        println("operator state : ", element)
      }
    }
  }
}
