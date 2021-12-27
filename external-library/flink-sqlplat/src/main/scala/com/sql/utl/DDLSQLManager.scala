package com.sql.utl

object DDLSQLManager {
  def createStreamFromKafka(broker: String,
                            topic: String,
                            tableName: String,
                            groupID: String,
                            format : String = "json"): String = {
    s"""CREATE TABLE $tableName (
       |    msg VARCHAR
       |) WITH (
       |    'connector' = 'kafka',
       |    'topic' = '$topic',
       |    'scan.startup.mode' = 'latest-offset',
       |    'properties.bootstrap.servers' = '$broker',
       |    'properties.group.id' = '$groupID',
       |    'format' = '$format',
       |    'json.ignore-parse-errors' = 'true'
       |)""".stripMargin
  }

  def createPrintSink(tblName: String): String = {
    s"""CREATE TABLE ${tblName} (
       |msg VARCHAR,
       |uv BIGINT
       |) WITH (
       |'connector' = 'print'
       |)""".stripMargin
  }
}
