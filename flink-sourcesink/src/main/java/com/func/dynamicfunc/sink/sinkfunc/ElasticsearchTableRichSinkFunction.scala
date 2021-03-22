package com.func.dynamicfunc.sink.sinkfunc

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap

class ElasticsearchTableRichSinkFunction
    extends RichSinkFunction[RowData]
    with CheckpointedFunction {
  private var checkpointedState: ListState[RowData] = null // checkpoint state
  private val bufferedElements = new HashMap[String, RowData]() // buffer List

  var converter: DynamicTableSink.DataStructureConverter = null
  var options: ReadableConfig = null;
  var shcema: mutable.Buffer[RowType.RowField] = null;

  def this(converter: DynamicTableSink.DataStructureConverter,
           options: ReadableConfig,
           shcema: DataType) {
    this()
    this.converter = converter;
    this.options = options;
    this.shcema = shcema.getLogicalType.asInstanceOf[RowType].getFields.asScala;
  }

  override def open(parameters: Configuration): Unit = {
    //    val myFile = getRuntimeContext.getDistributedCache.getFile("es_pack_file")
    //    ElasticsearchHandler.getGlobalEsClient(
    //      options.get(ElasticsearchFactory.ES_URL),
    //      options.get(ElasticsearchFactory.ES_CLUSTERNAME),
    //      options.get(ElasticsearchFactory.ES_PASSW),
    //      myFile.getPath)
  }

  override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
    println(value)
    value.getRowKind match {
      case RowKind.INSERT =>
        bufferedElements.put(value.getString(0).toString, value)
      //        val doc =
      //        MongoDBFactory.bulkWrite(collection, Seq(doc))
      case RowKind.UPDATE_AFTER =>
        bufferedElements.put(value.getString(0).toString, value)
        bufferedElements
      //        val doc = row2document(value)
      //        MongoDBFactory.bulkWrite(collection, Seq(doc))
      case _ =>
    }

  }

  /**
   *
   * @param context
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    bufferedElements.foreach {
      case (_, rowdata) =>
        println("snapshotState: " + rowdata)
        checkpointedState.add(rowdata)
    }
    bufferedElements.clear()
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
      println("--- initializeState ---", bufferedElements.size)
    }
  }
}
