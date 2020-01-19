package com.flink.learn.sql.entry
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
object FlinkSQLSourceKafka {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)
    tEnv.sqlUpdate(createFromkafkasql)
    tEnv
      .sqlQuery(s"""select * from ssp_sdk_report""")
      .toAppendStream[Row]
      .print()

    tEnv.execute("sql test")
  }




  val createFromkafkasql = s"""CREATE TABLE ssp_sdk_report (
               |    lon VARCHAR,
               |    rideTime VARCHAR
               |) WITH (
               |    'connector.type' = 'kafka',
               |    'connector.topic' = 'mobssprequestlog',
               |    'connector.startup-mode' = 'earliest-offset',
               |    'connector.properties.1.key' = 'bootstrap.servers',
               |    'connector.properties.1.value' = '10.21.33.28:9092,10.21.33.29:9092',
               |    'update-mode' = 'append',
               |    'format.type' = 'csv',
               |    'format.fields.0.name' = 'lon',
               |  'format.fields.0.type' = 'string',
               |  'format.fields.1.name' = 'rideTime',
               |  'format.fields.1.type' = 'string'
               |)""".stripMargin

  val createFromMysql = s"""CREATE TABLE ssp_sdk_report (
                    |    bid_req_num BIGINT,
                    |    md_key VARCHAR
                    |) WITH (
                    |    'connector.type' = 'jdbc',
                    |    'connector.url' = 'jdbc:mysql://10.18.97.129:3306/bgm_ssp_test',
                    |    'connector.table' = 'ssp_sdk_report',
                    |    'connector.username' = 'root',
                    |    'connector.password' = '123456',
                    |    'connector.write.flush.max-rows' = '1'
                    |)""".stripMargin
}
