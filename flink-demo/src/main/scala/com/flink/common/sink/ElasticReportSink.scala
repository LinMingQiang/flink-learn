package com.flink.common.sink

import java.util.Date

import com.flink.common.bean.CaseClassUtil.ReportInfo
import com.flink.common.param.PropertiesUtil
import com.flink.learn.dbutil.ElasticsearchHandler
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.transport.TransportClient

import scala.collection.JavaConversions._
import scala.collection.mutable

class ElasticReportSink(size: Int, interval: Long)
    extends RichSinkFunction[ReportInfo]
    with CheckpointedFunction {
  var client: TransportClient = null
  private var checkpointedState: ListState[ReportInfo] = _ // checkpoint state
  private val bufferedElements = mutable.HashMap[String, ReportInfo]() // buffer List
  var nextTime = 0L

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
    PropertiesUtil.init(parame)
    client = ElasticsearchHandler.getEsClient(PropertiesUtil.ES_HOSTS,
                                              PropertiesUtil.ES_CLUSTERNAME)
  }
  // (day, hour, buyerId, appid, adslot_id, policyId)
  /**
    *
    * @param value
    */
  override def invoke(value: ReportInfo): Unit = {
    try {
      bufferedElements.put(value.keybyKey, value) // 每次都保存最新的
      if (new Date().getTime > nextTime || bufferedElements.size > size) { // 每个一分钟提交一次
        println(" bulk commit start " , bufferedElements.size)
        nextTime = new Date().getTime + 1000 * interval
        val bulk = client.prepareBulk()
        bufferedElements.foreach(x => {
          val index = new IndexRequest()// ElasticDocAssembleUtil.getUpsertIndex(x._2)
          bulk.add(index)
        })
        if(bulk.numberOfActions()>0){
          if(bulk.get().hasFailures){
            println(" ElasticReportSink : bulk fail retry ")
            println("Retry is Fail ? -> " , bulk.get().hasFailures)
          }
        }
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
