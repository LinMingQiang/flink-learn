package com.flink.learn.sql.entry
import com.flink.common.core.FlinkLearnPropertiesUtil
import com.flink.learn.sql.common.SQLManager
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row
object FlinkSQLDDLEntry {
  FlinkLearnPropertiesUtil.init(
    "/Users/eminem/workspace/flink/flink-learn/dist/conf/application.properties",
    "test")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sett =
      EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tEnv = StreamTableEnvironment.create(env, sett)
    csvSink(tEnv)
    tEnv.execute("sql test")

  }

  /**
    *
    * @param tEnv
    */
  def csvSink(tEnv: StreamTableEnvironment): Unit = {
    val sink = new CsvTableSink(
      "/Users/eminem/workspace/flink/flink-learn/checkpoint/data", // output path
      "|", // optional: delimit files by '|'
      1, // optional: write to a single file
      WriteMode.OVERWRITE
    ) // optional: override existing files

    tEnv.registerTableSink("csvOutputTable",
                           // specify table schema
                           Array[String]("bid_req_num", "md_key"),
                           Array[TypeInformation[_]](Types.LONG, Types.STRING),
                           sink)
    createTable(tEnv, SQLManager.createFromMysql("ssp_sdk_report"))
    tEnv.sqlUpdate(
      s"""insert into csvOutputTable select * from ssp_sdk_report""")
    // readFromMysql(tEnv).insertInto("csvOutputTable")
  }

  /**
    *
    * @param tEnv
    * @param sql
    */
  def createTable(tEnv: StreamTableEnvironment, sql: String): Unit = {
    tEnv.sqlUpdate(sql)
  }

  /**
    *
    */
  def readFromMysql(tEnv: StreamTableEnvironment): Table = {
    createTable(tEnv, SQLManager.createFromMysql("ssp_sdk_report"))
    tEnv
      .sqlQuery(s"""select * from ssp_sdk_report""")
    //  .toAppendStream[Row].print
  }
}
