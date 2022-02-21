package com.sql.manager

import com.flink.common.core.FlinkLearnPropertiesUtil

object DDLSourceSQLManager {

  /**
   *
   * @param topic
   * @param tableName
   * @return
   */

  def createStreamFromKafka(broker: String,
                            topic: String,
                            tableName: String,
                            groupID: String,
                            format : String = "json"): String = {
    s"""CREATE TABLE $tableName (
       |    topic VARCHAR METADATA FROM 'topic',
       |    `offset` bigint METADATA,
       |    rowtime TIMESTAMP(3),
       |    msg VARCHAR,
       |    name varchar,
       |    age int,
       |    proctime AS PROCTIME(),
       |    `dt` as DATE_FORMAT(rowtime, 'yyyy-MM-dd'),
       |    WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND
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

  def createPrintSinkTbl(printlnSinkTbl: String): String = {
    s"""CREATE TABLE ${printlnSinkTbl} (
       |msg VARCHAR,
       |proctime TIMESTAMP(3),
       |rowtime TIMESTAMP(3)
       |) WITH (
       |'connector' = 'print'
       |)""".stripMargin
  }

}
