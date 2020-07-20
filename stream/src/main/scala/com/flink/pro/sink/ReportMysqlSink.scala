package com.flink.pro.sink

import java.sql.{Connection, PreparedStatement}
import java.util.Date

import scala.collection.JavaConverters._
import com.flink.common.dbutil.MysqlHandler
import com.flink.pro.common.CaseClassUtil.ReportInfo
import com.flink.pro.common.StreamPropertiesUtil
import com.flink.pro.common.StreamPropertiesUtil._
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{
  FunctionInitializationContext,
  FunctionSnapshotContext
}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext
import org.slf4j.LoggerFactory
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

class ReportMysqlSink(size: Int,
                      interval: Long,
                      insertSql: String,
                      inserMysql: (ReportInfo, PreparedStatement) => Unit)
    extends RichSinkFunction[ReportInfo]
    with CheckpointedFunction {
  val _log = LoggerFactory.getLogger(this.getClass)
  private var checkpointedState: ListState[ReportInfo] = _ // checkpoint state
  private val bufferedElements = mutable.HashMap[String, ReportInfo]() // buffer List
  var nextTime = 0L // 下一次提交时间
  var taskIndex = 0 // 当前slot的id
  var conn: Connection = _
  var prest: PreparedStatement = _

  /**
    * 初始化配置
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    taskIndex = this.getRuntimeContext
      .asInstanceOf[StreamingRuntimeContext]
      .getIndexOfThisSubtask
    val parame = getRuntimeContext()
      .getExecutionConfig()
      .getGlobalJobParameters()
      .asInstanceOf[ParameterTool]
    StreamPropertiesUtil.init(parame)
    try {
      conn = MysqlHandler.getMysqlConn(JDBC, USER, PASSWD)
      conn.setAutoCommit(false)
      prest = conn.prepareStatement(insertSql);
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }

  /**
    * 提交方式： 每分钟提交一次或者多大提交一次
    * @param value
    */
  override def invoke(value: ReportInfo): Unit = {
    try {
      bufferedElements.put(value.keybyKey, value) // 每次都保存最新的
      if (new Date().getTime > nextTime || bufferedElements.size > size) { // 每个一分钟提交一次
        nextTime = new Date().getTime + 1000 * interval
        bufferedElements.foreach {
          case (_, info) =>
            inserMysql(info, prest)
        }
        commitBulk()
        bufferedElements.clear()
      }
    } catch {
      case e: Throwable =>
        if (prest.isClosed) {
          prest = conn.prepareStatement(insertSql);
        }
        _log.error(s"invoke : ${e.toString}")
    }
  }

  /**
    * 为了防止，数据后面不更新了，导致这批缓存不更新，在做快照的时候提交一次
    * @param functionSnapshotContext
    */
  override def snapshotState(
      functionSnapshotContext: FunctionSnapshotContext): Unit = {
    try {
      checkpointedState.clear()
      for ((_, element) <- bufferedElements) {
        checkpointedState.add(element)
        inserMysql(element, prest)
      }
      commitBulk()
    } catch {
      case e: Throwable =>
        if (prest.isClosed) {
          prest = conn.prepareStatement(insertSql);
        }
    } finally {
      bufferedElements.clear()
      checkpointedState.clear() // 这部分提交成功了，那就把chk state再删掉，没必要chk
    }

  }

  /**
    * 初始化恢复
    * @param functionInitializationContext
    */
  override def initializeState(
      functionInitializationContext: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[ReportInfo](
      "ReportInfo",
      createTypeInformation[ReportInfo])
    checkpointedState = functionInitializationContext.getOperatorStateStore
      .getListState(descriptor)
    if (functionInitializationContext.isRestored) {
      for (element <- checkpointedState.get().asScala) {
        if (element.groupKey(5) != null)
          bufferedElements += (element.keybyKey -> element)
        _log.info(s"restore : $element")
      }
    }
  }



  def commitBulk(): Unit = {
    prest.executeBatch()
    conn.commit()
    prest.clearBatch()
  }
  override def close(): Unit = {
    if (prest != null) prest.close
    if (conn != null) {
      conn.close()
    }
    super.close()
  }
}
