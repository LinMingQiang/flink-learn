package com.func.dynamicfunc.sink.sinkfunc

import com.factory.dynamicfactory.sink.ElasticsearchFactory
import com.flink.common.dbutil.ElasticsearchHandler
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{
  RichSinkFunction,
  SinkFunction
}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap

class ElasticsearchTableRichSinkFunction
  extends RichSinkFunction[RowData]
    with CheckpointedFunction {
  private var checkpointedState: ListState[RowData] = null // checkpoint state
  private val bufferedElements = new HashMap[String, RowData]() // buffer List
  var client: PreBuiltXPackTransportClient = null

  var converter: DynamicTableSink.DataStructureConverter = null
  var options: ReadableConfig = null;
  var shcema: mutable.Buffer[RowType.RowField] = null;
  var ES_ROWKEY_INDEX = 0
  var COMMIT_SIZE = 1000L
  var nextCommitTime = 0L
  var COMMIT_INTERVAL = 0L
  var ES_INDEX_NAME = ""

  def this(converter: DynamicTableSink.DataStructureConverter,
           options: ReadableConfig,
           shcema: DataType) {
    this()
    this.converter = converter;
    this.options = options;
    this.shcema = shcema.getLogicalType.asInstanceOf[RowType].getFields.asScala;
    for (i <- 0 to (this.shcema.size - 1)) {
      if (this
        .shcema(i)
        .getName
        .equals(options.get(ElasticsearchFactory.ES_ROWKEY))) {
        ES_ROWKEY_INDEX = i
      }
    }
    COMMIT_SIZE = options.get(ElasticsearchFactory.ES_COMMIT_SIZE)
    COMMIT_INTERVAL = options.get(ElasticsearchFactory.ES_COMMIT_INTERVAL) * 1000L
    nextCommitTime = System.currentTimeMillis() + COMMIT_INTERVAL
    ES_INDEX_NAME = options.get(ElasticsearchFactory.ES_INDEX)
  }

  override def open(parameters: Configuration): Unit = {
//    val myFile = getRuntimeContext.getDistributedCache.getFile("es_pack_file")
//    ElasticsearchHandler.getGlobalEsPreClient(
//      options.get(ElasticsearchFactory.ES_URL),
//      options.get(ElasticsearchFactory.ES_CLUSTERNAME),
//      options.get(ElasticsearchFactory.ES_PASSW),
//      myFile.getPath)
  }

  override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
    value.getRowKind match {
      case RowKind.INSERT =>
        bufferedElements.put(value.getString(ES_ROWKEY_INDEX).toString, value)
      case RowKind.UPDATE_AFTER =>
        bufferedElements.put(value.getString(ES_ROWKEY_INDEX).toString, value)
      case _ =>
    }
    //
    if (bufferedElements.size >= COMMIT_SIZE || System.currentTimeMillis() >= nextCommitTime) {
      println("commit : " + bufferedElements.size)
//      val bulk = client.prepareBulk()
//      bufferedElements.foreach {
//        case (_, rowdata) =>
//          val updateReq =
//            ElasticsearchHandler.createUpdateReqestFromRowData(ES_INDEX_NAME,
//              ES_ROWKEY_INDEX,
//              rowdata,
//              shcema)
//          bulk.add(updateReq)
//      }
//      bulk.numberOfActions()
//      bufferedElements.clear()
//      nextCommitTime = nextCommitTime + COMMIT_INTERVAL
    }

  }

  /**
   *
   * @param context
   * 这个地方其实不需要做checkpointedState，因为这里也会提交一次。所以不会出现任务失败丢失的问题。
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    //    checkpointedState.clear()
    val bulk = client.prepareBulk();
    bufferedElements.foreach {
      case (_, rowdata) =>
        //        checkpointedState.add(rowdata)
        val updateReq =
          ElasticsearchHandler.createUpdateReqestFromRowData(ES_INDEX_NAME,
            ES_ROWKEY_INDEX,
            rowdata,
            shcema)
        bulk.add(updateReq)
    }
    bulk.numberOfActions()
    bufferedElements.clear()
    //    checkpointedState.clear()
  }

  /**
   *
   * @param context
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor(
      "buffered-elements",
      TypeInformation.of(classOf[RowData]))
    checkpointedState = context.getOperatorStateStore.getListState(descriptor)
    if (context.isRestored) {
      for (element <- checkpointedState.get().asScala) {
        bufferedElements.put(element.getString(0).toString, element)
      }
//      println("--- initializeState ---", bufferedElements.size)
    }
  }
}
