package com.func.dynamicfunc.sink.sinkfunc

import com.factory.dynamicfactory.sink.ClickHouseDynamicTableSinkFactory
import com.flink.common.dbutil.{ClickhouseFactory, MysqlJdbcHandler}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.{Configuration, ReadableConfig}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.logical._
import org.apache.flink.types.RowKind
import org.slf4j.LoggerFactory

import java.sql.{Connection, PreparedStatement}
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.HashMap

class ClickhouseTableRichSinkFunction
  extends RichSinkFunction[RowData]
    with CheckpointedFunction {
  val log = LoggerFactory.getLogger(classOf[ClickhouseTableRichSinkFunction]);
  private var checkpointedState: ListState[RowData] = null // checkpoint state
  private var bufferedElements = new util.ArrayList[RowData]() // buffer List
  var converter: DynamicTableSink.DataStructureConverter = null
  var options: ReadableConfig = null;
  var conn: Connection = null
  var shcema: mutable.Buffer[RowType.RowField] = null;
  var COMMIT_SIZE = 1000
  var nextCommitTime = 0L
  var COMMIT_INTERVAL = 0L
  var TABLE_NAME = ""
  var JDBC_URL = ""
  var USER = ""
  var PASSW = ""
  var INSERT_SQL = ""

  /**
   * @param converter
   * @param options 参数
   * @param shcema  字段类型和顺序
   */
  def this(converter: DynamicTableSink.DataStructureConverter,
           options: ReadableConfig,
           shcema: DataType) {
    this()
    this.shcema = shcema.getLogicalType.asInstanceOf[RowType].getFields.asScala;
    this.converter = converter;
    this.options = options;
    this.JDBC_URL = options.get(ClickHouseDynamicTableSinkFactory.URL)
    this.USER = options.get(ClickHouseDynamicTableSinkFactory.USERNAME)
    this.PASSW = options.get(ClickHouseDynamicTableSinkFactory.PASSWORD)
    this.COMMIT_SIZE = options.get(ClickHouseDynamicTableSinkFactory.SINK_BUFFER_FLUSH_MAX_ROWS)
    this.COMMIT_INTERVAL = options.get(ClickHouseDynamicTableSinkFactory.SINK_BUFFER_FLUSH_INTERVAL) * 1000L
    this.nextCommitTime = System.currentTimeMillis() + COMMIT_INTERVAL
    this.TABLE_NAME = options.get(ClickHouseDynamicTableSinkFactory.TABLE_NAME)
    this.INSERT_SQL = assemblySQL()

  }


  /**
   * 初始化连接
   *
   * @param parameters
   */
  override def open(parameters: Configuration): Unit = {
    this.conn = ClickhouseFactory.getGlobalConn(JDBC_URL, USER, PASSW)
    conn.setAutoCommit(false)
  }

  /**
   *
   * @param value : 固定第一个字段就是Rowkey
   * @param context
   */
  override def invoke(value: RowData, context: SinkFunction.Context): Unit = {
    value.getRowKind match {
      case RowKind.INSERT =>
        bufferedElements.add(value)
      case RowKind.UPDATE_AFTER =>
        bufferedElements.add(value)
      case _ =>
    }
    if (bufferedElements.size >= COMMIT_SIZE || System.currentTimeMillis() >= nextCommitTime) {
      commitDataToMysql()
      nextCommitTime = nextCommitTime + COMMIT_INTERVAL
    }

  }

  /**
   * 主要为了当前key没有数据来了之后的数据提交和清理问题
   *
   * @param context
   */
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    if (bufferedElements.size > 0) {
      commitDataToMysql()
    }
    if (bufferedElements.size > 0) {
      checkpointedState.clear()
      bufferedElements.forEach(x => {
        checkpointedState.add(x)
      })
    }
  }

  override def close(): Unit = {
    log.warn(TABLE_NAME + " close : " + bufferedElements.size)
    if (bufferedElements.size > 0) {
      commitDataToMysql()
    }
    bufferedElements.clear()
  }

  /**
   *
   * @param context
   */
  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor(
      "MySqlTableRichSinkFunction",
      TypeInformation.of(classOf[RowData]))
    checkpointedState = context.getOperatorStateStore.getListState(descriptor)
    if (context.isRestored) {
      for (element <- checkpointedState.get().asScala) {
        bufferedElements.add(element)
      }
    }
  }


  /**
   * 组装sql
   *
   * @return
   */
  def assemblySQL(): String = {
    var sql =
      s"""insert into
         | `${TABLE_NAME}`(""".stripMargin
    var values = " values("
    shcema.foreach(x => {
      sql += (x.getName + ",")
      values += "?,"
    })
    sql = sql.substring(0, sql.size - 1)
    values = values.substring(0, values.size - 1)
    sql += ")"
    values += ")"
    sql += values
    sql
  }

  /**
   * 组装
   *
   * @param prepareStatement
   * @param rowData
   */
  def assemblyPrepareStatement(prepareStatement: PreparedStatement, rowData: RowData): Unit = {
    for (i <- 0 until shcema.size) {
      shcema(i).getType match {
        case _: VarCharType =>
          if (rowData.getString(i) == null) {
            prepareStatement.setString(i + 1, "-1")
          } else {
            prepareStatement.setString(i + 1, rowData.getString(i).toString)
          }
        case _: DoubleType =>
          prepareStatement.setDouble(i + 1, rowData.getDouble(i))
        case _: BigIntType =>
          prepareStatement.setLong(i + 1, rowData.getLong(i))
        case _: IntType =>
          prepareStatement.setInt(i + 1, rowData.getInt(i))
        case _ => println("类型不对 。。。")
      }
    }
  }

  /**
   * 提交任务
   */
  def commitDataToMysql(retryTime: Int = 3): Unit = {
    val prepareStatement = conn.prepareStatement(INSERT_SQL)
    bufferedElements.forEach{
      rowdata =>
        assemblyPrepareStatement(prepareStatement, rowdata)
        prepareStatement.addBatch()
    }
    try {
      prepareStatement.executeBatch()
      conn.commit()
      prepareStatement.clearBatch()
    } catch {
      case e: Throwable =>
        log.error(TABLE_NAME + " ：mysql commit error : " + e.toString)
        prepareStatement.close()
        conn.close()
        this.open(null)
        if (retryTime > 0) {
          commitDataToMysql(retryTime - 1)
        }
    }
    bufferedElements.clear()
    prepareStatement.close()
  }
}
