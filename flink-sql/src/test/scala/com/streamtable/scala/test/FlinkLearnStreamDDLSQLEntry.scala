package com.streamtable.scala.test

import com.flink.learn.sql.common.{DDLQueryOrSinkSQLManager, DDLSourceSQLManager, TableSinkManager}
import com.flink.learn.sql.func.DdlTableFunction.Split
import com.flink.learn.sql.func.{StrToLowOrUpScalarFunction, WeightedAvgAggregateFunction}
import com.flink.learn.test.common.FlinkStreamTableCommonSuit
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

class FlinkLearnStreamDDLSQLEntry extends FlinkStreamTableCommonSuit {



  test("aggFunc") {
    tableEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tableEnv.registerFunction("split", new Split(","))
    tableEnv.registerFunction("wAvg", new WeightedAvgAggregateFunction())
    tableEnv
      .sqlQuery(
        s"""SELECT username,wAvg(cast(splita as bigint), cast(splita as int)) FROM test,
                   | LATERAL TABLE(split(url)) as T(splita, word_size)
                   | group by username """.stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    tableEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }

  /**
    *  function
    */
  test("scalarFunc") {
    tableEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tableEnv.registerFunction("strtouporlow", StrToLowOrUpScalarFunction)
    tableEnv
      .sqlQuery(s"""SELECT username,strtouporlow(url) FROM test""".stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    tableEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }

  /**
    * 一行转多行多列 Tablefunction
    */
  test("lateralTbl") {
    // {"username":"1","url":"1,22,333","tt": 1588144690402}
    tableEnv.registerFunction("split", new Split(","))
    tableEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tableEnv
      .sqlQuery(s"""SELECT username, url,splita,word_size FROM test,
           | LATERAL TABLE(split(url)) as T(splita, word_size)""".stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    tableEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }

  /**
    * 窗口设置。2分钟一个窗口，只有2分钟到了才会有输出
    */
  test("ddlEventTimeWatermark") {
    //      {"username":"1","url":"111","tt": 1588131008676}
    tableEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tableEnv
      .sqlQuery(
        s"""select username,Row(count(1)) from test group by username""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    // 窗口统计，统计2分钟的窗口
    tableEnv
      .sqlQuery(DDLQueryOrSinkSQLManager.tumbleWindowSink("test"))
      .toRetractStream[Row]
      .print
    tableEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }

  /**
    * 简单示例
    */
  test("ddlSample") {
    tableEnv.sqlUpdate(
      DDLSourceSQLManager.createStreamFromKafka_CSV("localhost:9092",
                                                    "localhost:2181",
                                                    "test",
                                                    "test",
                                                    ",",
                                                    "test"))
    tableEnv
      .sqlQuery(s"""select id,count(*) num from test group by id""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    // insertIntoCsvTbl(tEnv)
    tableEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }

  /**
    *
    */
  test("insertIntoCsvTbl") {
    TableSinkManager.registerCsvTableSink(
      tableEnv,
      "csvSinkTbl",
      Array[String]("bid_req_num", "md_key"),
      Array[TypeInformation[_]](Types.LONG, Types.STRING),
      "/Users/eminem/workspace/flink/flink-learn/checkpoint/data", // output path
      "|", // optional: delimit files by '|'
      1, // optional: write to a single file
      WriteMode.OVERWRITE
    )
    tableEnv.sqlUpdate(
      s"""insert into csvSinkTbl select * from ssp_sdk_report""")
    // tEnv.sqlQuery(s"""select id,count(*) num from test group by id""").insertInto("csvSinkTbl")
    tableEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }
}
