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
                            topic: String,
                            tableName: String,
                            groupID: String,
                            format : String = "json"): String = {
    s"""CREATE TABLE $tableName (
       |    topic VARCHAR METADATA FROM 'topic',
       |    `offset` bigint METADATA,
       |    rowtime TIMESTAMP(3),
       |    msg VARCHAR,
       |    proctime AS PROCTIME(),
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
       |msg VARCHAR,
       |cnt BIGINT
       |) WITH (
       |'connector.type' = 'printsink',
       |'println.prefix'='>> : '
       |)""".stripMargin
  }

  def createCustomMongoSink(tblName: String): String = {
    s"""CREATE TABLE ${tblName} (
       |id VARCHAR,
       |msg VARCHAR,
       |uv BIGINT
       |) WITH (
       |'connector' = 'custom-mongo',
       |'mongourl'='localhost:27017',
       |'mongouser'='admin',
       |'mongopassw'='123456',
       |'mongodb'='test',
       |'collection'='runoob'
       |)""".stripMargin
  }

  def createHbaseLookupSource(tblName: String): String = {
    s"""CREATE TABLE ${tblName} (
       |word VARCHAR
       |) WITH (
       |'connector' = 'custom-hbase-lookup'
       |)""".stripMargin
  }

  /**
   * 新版的是connector,旧版是 connector.type
   * @param printlnSinkTbl
   * @return
   */
  def createDynamicPrintlnRetractSinkTbl(printlnSinkTbl: String): String = {
    s"""CREATE TABLE ${printlnSinkTbl} (
       |msg VARCHAR,
       |cnt BIGINT
       |) WITH (
       |'connector' = 'printRetract'
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

  def createFromMysql(tblName: String) =
    s"""CREATE TABLE ${tblName} (
       |    msg VARCHAR,
       |    cnt BIGINT,
       |    PRIMARY KEY (msg) NOT ENFORCED
       |) WITH (
       |    'connector' = 'jdbc',
       |    'url' = '${FlinkLearnPropertiesUtil.MYSQL_HOST}',
       |    'table-name' = '${tblName}',
       |    'username' = '${FlinkLearnPropertiesUtil.MYSQL_USER}',
       |    'password' = '${FlinkLearnPropertiesUtil.MYSQL_PASSW}',
       |    'sink.buffer-flush.max-rows' = '1'
       |)""".stripMargin
  def createCustomESSink(tblName: String): String = {
    s"""CREATE TABLE ${tblName} (
       |id VARCHAR,
       |msg VARCHAR,
       |uv BIGINT
       |) WITH (
       |'connector' = 'custom-es',
       |'es.address'='localhost:27017',
       |'es.clustername'='admin',
       |'es.passw'='123456',
       |'es.index'='test',
       |'es.commit.size'='runoob'
       |)""".stripMargin
  }
}
