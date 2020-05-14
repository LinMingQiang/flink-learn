package com.flink.learn.sql.stream.entry
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import com.flink.common.core.{
  EnvironmentalKey,
  FlinkEvnBuilder,
  FlinkLearnPropertiesUtil
}
import com.flink.learn.sql.common.{
  DDLQueryOrSinkSQLManager,
  DDLSourceSQLManager,
  TableSinkManager
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import FlinkLearnPropertiesUtil._
import com.flink.learn.sql.func.DdlTableFunction.Split
import com.flink.learn.sql.func.{
  StrToLowOrUpScalarFunction,
  WeightedAvgAggregateFunction
}
object FlinkLearnStreamDDLSQLEntry {

  /**
    * 使用sql的方式连接source
    * @param args
    */
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "FlinkLearnStreamDDLSQLEntry")
    val tEnv = FlinkEvnBuilder.buildStreamTableEnv(param,
                                                   CHECKPOINT_PATH,
                                                   10000,
                                                   Time.minutes(1),
                                                   Time.minutes(6))
    // 创建source 表
    //ddlSample(tEnv)
    // ddlEventTimeWatermark(tEnv)
    // lateralTbl(tEnv)
    // scalarFunc(tEnv)
    aggFunc(tEnv)
    tEnv.execute("FlinkLearnStreamDDLSQLEntry")
  }

  /**
    * 自定义聚合类
    * @param tEnv
    */
  def aggFunc(tEnv: StreamTableEnvironment): Unit = {
    tEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tEnv.registerFunction("split", new Split(","))
    tEnv.registerFunction("wAvg", new WeightedAvgAggregateFunction())
    tEnv
      .sqlQuery(
        s"""SELECT username,wAvg(cast(splita as bigint), cast(splita as int)) FROM test,
                   | LATERAL TABLE(split(url)) as T(splita, word_size)
                   | group by username """.stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
  }

  /**
    * 简单的自定义函数
    * @param tEnv
    */
  def scalarFunc(tEnv: StreamTableEnvironment): Unit = {

    tEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tEnv.registerFunction("strtouporlow", StrToLowOrUpScalarFunction)
    tEnv
      .sqlQuery(s"""SELECT username,strtouporlow(url) FROM test""".stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
  }

  /**
    * 一行转多行多列 Tablefunction
    * @param tEnv
    */
  def lateralTbl(tEnv: StreamTableEnvironment): Unit = {
    // {"username":"1","url":"1,22,333","tt": 1588144690402}
    tEnv.registerFunction("split", new Split(","))
    tEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tEnv
      .sqlQuery(s"""SELECT username, url,splita,word_size FROM test,
           | LATERAL TABLE(split(url)) as T(splita, word_size)""".stripMargin)
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print

  }

  /**
    * 窗口设置。2分钟一个窗口，只有2分钟到了才会有输出
    * @param tEnv
    */
  def ddlEventTimeWatermark(tEnv: StreamTableEnvironment): Unit = {
    //      {"username":"1","url":"111","tt": 1588131008676}
    tEnv.sqlUpdate(DDLSourceSQLManager.ddlTumbleWindow("test", "test"))
    tEnv
      .sqlQuery(
        s"""select username,Row(count(1)) from test group by username""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    // 窗口统计，统计2分钟的窗口
    tEnv
      .sqlQuery(DDLQueryOrSinkSQLManager.tumbleWindowSink("test"))
      .toRetractStream[Row]
      .print
  }

  /**
    * 简单示例
    * @param tEnv
    */
  def ddlSample(tEnv: StreamTableEnvironment): Unit = {
    tEnv.sqlUpdate(DDLSourceSQLManager.createStreamFromKafka("test", "test"))
    tEnv
      .sqlQuery(s"""select id,count(*) num from test group by id""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print
    // insertIntoCsvTbl(tEnv)
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
