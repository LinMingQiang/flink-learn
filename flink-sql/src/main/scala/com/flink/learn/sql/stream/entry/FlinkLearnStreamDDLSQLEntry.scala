package com.flink.learn.sql.stream.entry
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import com.flink.common.core.{FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.learn.sql.common.SQLManager
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, StreamQueryConfig}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row

object FlinkLearnStreamDDLSQLEntry {

  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(
      "/Users/eminem/workspace/flink/flink-learn/dist/conf/application.properties",
      "test")
    val env = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param,FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
      10000) // 1 min
    val sett =
      EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tEnv = StreamTableEnvironment.create(env, sett)
    // state 保存时间 minTime maxTime
    val queryConfig = new StreamQueryConfig()
      .withIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    createTable(tEnv, SQLManager.createStreamFromKafka("test", "test"))
    tEnv
      .toRetractStream[Row](
        tEnv
          .sqlQuery(s"""select id,count(*) from test group by id"""),
        queryConfig)
      .filter(_._1)
      .map(r => { r._2 })
      .print
    // insertIntoTbl(tEnv)
    tEnv.execute("sql test")
  }

  /**
    *
    * @param tEnv
    */
  def insertIntoTbl(tEnv: StreamTableEnvironment): Unit = {
    val sink = new CsvTableSink(
      "/Users/eminem/workspace/flink/flink-learn/checkpoint/data", // output path
      "|", // optional: delimit files by '|'
      1, // optional: write to a single file
      WriteMode.OVERWRITE
    )

    tEnv.registerTableSink("csvOutputTable",
                           // specify table schema
                           Array[String]("bid_req_num", "md_key"),
                           Array[TypeInformation[_]](Types.LONG, Types.STRING),
                           sink)
    tEnv.sqlUpdate(
      s"""insert into csvOutputTable select * from ssp_sdk_report""")
    // tEnv.sqlQuery(s"""select * from test""").insertInto("csvOutputTable")
  }

  /**
    *
    * @param tEnv
    * @param sql
    */
  def createTable(tEnv: StreamTableEnvironment, sql: String): Unit = {
    tEnv.sqlUpdate(sql)
  }
}
