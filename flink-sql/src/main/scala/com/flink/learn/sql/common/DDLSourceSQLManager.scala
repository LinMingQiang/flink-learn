package com.flink.learn.sql.common

import com.flink.common.core.FlinkLearnPropertiesUtil

object DDLSourceSQLManager {

  /**
    * csv格式 的输入
    *
    * @param broker
    * @param zk
    * @param topic
    * @param tableName
    * @param delimiter
    * @param groupID
    * @return
    */
  def createStreamFromKafka_CSV(broker: String,
                                zk: String,
                                topic: String,
                                tableName: String,
                                delimiter: String,
                                groupID: String) =
    s"""CREATE TABLE $tableName (
       |    topic VARCHAR,
       |    msg VARCHAR
       |) WITH (
       |'connector' = 'kafka-0.10',
       |    'topic' = '$topic',
       |    'scan.startup.mode' = 'latest-offset',
       |    'properties.bootstrap.servers' = '$broker',
       |    'properties.group.id' = '$groupID',
       |    'format' = 'csv',
       |    'csv.field-delimiter' = '$delimiter',
       |    'csv.ignore-parse-errors' = 'true'
       |)""".stripMargin

  // -- earliest-offset /  group-offsets
  def createStreamFromKafkaProcessTime(broker: String,
                                       zk: String,
                                       topic: String,
                                       tableName: String,
                                       groupID: String): String = {
    s"""CREATE TABLE $tableName (
       |    id VARCHAR,
       |    name VARCHAR,
       |    age INT,
       |    proctime as PROCTIME()
       |) WITH (
          'connector' = 'kafka-0.10',
       |    'topic' = '$topic',
       |    'scan.startup.mode' = 'latest-offset',
       |    'properties.bootstrap.servers' = '$broker',
       |    'properties.group.id' = '$groupID',
       |    'format' = 'json',
       |    'json.fail-on-missing-field' = 'false',
       |    'json.ignore-parse-errors' = 'true'
       |)""".stripMargin
  }

  // -- earliest-offset /  group-offsets
  def createStreamFromKafkaEventTime(broker: String,
                                     zk: String,
                                     topic: String,
                                     tableName: String,
                                     groupID: String): String = {
    s"""CREATE TABLE $tableName (
       |    id VARCHAR,
       |    name VARCHAR,
       |    age INT,
       |    etime TIMESTAMP(3),
       |    WATERMARK FOR etime AS etime - INTERVAL '5' SECOND
       |) WITH (
          'connector' = 'kafka-0.10',
       |    'topic' = '$topic',
       |    'scan.startup.mode' = 'latest-offset',
       |    'properties.bootstrap.servers' = '$broker',
       |    'properties.group.id' = '$groupID',
       |    'format' = 'json',
       |    'json.fail-on-missing-field' = 'false',
       |    'json.ignore-parse-errors' = 'true'
       |)""".stripMargin
  }

  /**
    *
    * @param topic
    * @param tableName
    * @return
    */
  def createStreamFromKafka(broker: String,
                            zk: String,
                            topic: String,
                            tableName: String,
                            groupID: String): String = {
    s"""CREATE TABLE $tableName (
       |    id VARCHAR,
       |    name VARCHAR,
       |    age INT
       |) WITH (
          'connector' = 'kafka-0.10',
       |    'topic' = '$topic',
       |    'scan.startup.mode' = 'latest-offset',
       |    'properties.bootstrap.servers' = '$broker',
       |    'properties.group.id' = '$groupID',
       |    'format' = 'json',
       |    'json.fail-on-missing-field' = 'false',
       |    'json.ignore-parse-errors' = 'true'
       |)""".stripMargin
  }

  /**
    * 窗口
    */
  def ddlTumbleWindow(topic: String, tableName: String): String = {
    s"""CREATE TABLE $tableName (
       |    username VARCHAR,
       |    url VARCHAR,
       |    tt bigint,
       |    ts AS TO_TIMESTAMP(FROM_UNIXTIME(tt / 1000, 'yyyy-MM-dd HH:mm:ss')),
       |    proctime as PROCTIME(),
       |     WATERMARK FOR ts AS ts - INTERVAL '20' SECOND
       |) WITH (
          'connector' = 'kafka-0.10',
       |    'topic' = '$topic',
       |    'scan.startup.mode' = 'latest-offset',
       |    'properties.bootstrap.servers' = 'localhost:9092',
       |    'properties.group.id' = 'test',
       |    'format' = 'json',
       |    'json.fail-on-missing-field' = 'false',
       |    'json.ignore-parse-errors' = 'true'
       |)""".stripMargin

  }

  def createCustomSinkTbl(printlnSinkTbl: String): String = {
    s"""CREATE TABLE ${printlnSinkTbl} (
       |topic VARCHAR,
       |ll BIGINT,
       |msg VARCHAR
       |) WITH (
       |'connector.type' = 'printsink',
       |'println.prefix'='>> : '
       |)""".stripMargin
  }

  def createCustomPrintlnRetractSinkTbl(printlnSinkTbl: String): String = {
    s"""CREATE TABLE ${printlnSinkTbl} (
       |topic VARCHAR,
       |msg VARCHAR,
       |ll BIGINT
       |) WITH (
       |'connector.type' = 'printsink_retract',
       |'println.prefix'='>> : '
       |)""".stripMargin
  }

  def createCustomHbaseSinkTbl(hbasesinkTbl: String): String = {
    s"""CREATE TABLE ${hbasesinkTbl} (
       |topic VARCHAR,
       |c BIGINT
       |) WITH (
       |'connector.type' = 'hbasesink'
       |)""".stripMargin
  }

  def createFromMysql(sourceTbl: String, targetTbl: String) =
    s"""CREATE TABLE ${targetTbl} (
       |    bid_req_num BIGINT,
       |    md_key VARCHAR
       |) WITH (
       |    'connector.type' = 'jdbc',
       |    'connector.url' = '${FlinkLearnPropertiesUtil.MYSQL_HOST}',
       |    'connector.table' = '${sourceTbl}',
       |    'connector.username' = '${FlinkLearnPropertiesUtil.MYSQL_USER}',
       |    'connector.password' = '${FlinkLearnPropertiesUtil.MYSQL_PASSW}',
       |    'connector.write.flush.max-rows' = '1'
       |)""".stripMargin

}
