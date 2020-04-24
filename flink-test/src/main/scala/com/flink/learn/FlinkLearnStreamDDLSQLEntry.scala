package com.flink.learn

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, StreamQueryConfig}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row

object FlinkLearnStreamDDLSQLEntry {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sett =
      EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tEnv = StreamTableEnvironment.create(env, sett)
    // state 保存时间 minTime maxTime
    val queryConfig = new StreamQueryConfig()
      .withIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    createTable(tEnv, createStreamFromKafka("test", "test"))
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
  def createStreamFromKafka(topic: String, tableName: String): String = {
    s"""CREATE TABLE $tableName (
       |    id VARCHAR,
       |    name VARCHAR,
       |    age INT
       |) WITH (
       |    'connector.type' = 'kafka',
       |    'connector.version' = '0.11',
       |    'connector.topic' = '$topic',
       |    'connector.startup-mode' = 'latest-offset',
       |    'connector.properties.zookeeper.connect' = 'localhost:2181',
       |    'connector.properties.bootstrap.servers' = 'localhost:9092',
       |    'connector.properties.group.id' = 'testGroup',
       |    'update-mode' = 'append',
       |    'format.type' = 'json',
       |    'format.derive-schema' = 'true'
       |)""".stripMargin
  }
}
