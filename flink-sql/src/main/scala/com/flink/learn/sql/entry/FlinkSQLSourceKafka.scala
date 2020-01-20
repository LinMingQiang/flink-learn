package com.flink.learn.sql.entry
import com.flink.learn.sql.common.SQLManager
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.factories.TableSourceFactory
import org.apache.flink.types.Row
object FlinkSQLSourceKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val sett = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tEnv = StreamTableEnvironment.create(env, sett)
    tEnv.sqlUpdate(SQLManager.createFromkafkasql)
    tEnv
      .sqlQuery(s"""select * from ssp_sdk_report""")
      .toAppendStream[Row]
      .print()
    tEnv.execute("sql test")
  }


}
