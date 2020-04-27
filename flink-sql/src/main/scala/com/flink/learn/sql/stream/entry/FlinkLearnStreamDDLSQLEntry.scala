package com.flink.learn.sql.stream.entry
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import com.flink.common.core.{FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.learn.sql.common.{SQLManager, TableSinkManager}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.{StreamQueryConfig}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row
import FlinkLearnPropertiesUtil._
object FlinkLearnStreamDDLSQLEntry {

  /**
    * 使用sql的方式连接source
    * @param args
    */
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(
      "/Users/eminem/workspace/flink/flink-learn/dist/conf/application.properties",
      "test")
    val tEnv = FlinkEvnBuilder.buildStreamTableEnv(param,
                                                   CHECKPOINT_PATH,
                                                   10000,
                                                   Time.minutes(1),
                                                   Time.minutes(6))
    // 创建source 表
    tEnv.sqlUpdate(SQLManager.createStreamFromKafka("test", "test"))
    tEnv
      .sqlQuery(s"""select id,count(*) num from test group by id""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    insertIntoCsvTbl(tEnv)
    tEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }

  /**
    *
    * @param tEnv
    */
  def insertIntoCsvTbl(tEnv: StreamTableEnvironment): Unit = {
    TableSinkManager.registerCsvTableSink(
      tEnv,
      "csvSinkTbl",
      Array[String]("bid_req_num", "md_key"),
      Array[TypeInformation[_]](Types.LONG, Types.STRING),
      "/Users/eminem/workspace/flink/flink-learn/checkpoint/data", // output path
      "|", // optional: delimit files by '|'
      1, // optional: write to a single file
      WriteMode.OVERWRITE
    )
    tEnv.sqlUpdate(s"""insert into csvSinkTbl select * from ssp_sdk_report""")
    // tEnv.sqlQuery(s"""select id,count(*) num from test group by id""").insertInto("csvSinkTbl")
  }
}
