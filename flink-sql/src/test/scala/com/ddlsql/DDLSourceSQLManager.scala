package com.ddlsql

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
       |    uid VARCHAR,
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
   *
   * @param topic
   * @param tableName
   * @return
   */

  def createStreamFromKafkaRowtime(broker: String,
                            topic: String,
                            tableName: String,
                            groupID: String,
                            format : String = "json"): String = {
    s"""CREATE TABLE $tableName (
       |    topic VARCHAR METADATA FROM 'topic',
       |    `offset` bigint METADATA,
       |    rowtime BIGINT,
       |    ts AS TO_TIMESTAMP(FROM_UNIXTIME(rowtime / 1000, 'yyyy-MM-dd HH:mm:ss')),
       |    msg VARCHAR,
       |    uid VARCHAR,
       |    proctime AS PROCTIME(),
       |    WATERMARK FOR ts AS ts - INTERVAL '10' SECOND
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
   * 创建 时态表，必须要有主见
   * @param broker
   * @param topic
   * @param tableName
   * @param groupID
   * @param format
   * @return
   */
  def createTemporalTable(broker: String,
                          topic: String,
                          tableName: String,
                          groupID: String,
                          format : String = "json"): String ={
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
       |    'format' = '$format'
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
       |'es.commit.intervalsec' = '10',
       |'es.commit.size'= '10',
       |'es.rowkey'='id'
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

  def createCustomRowMongoSink(tblName: String): String = {
    s"""CREATE TABLE ${tblName} (
       |id VARCHAR,
       |msg VARCHAR,
       |inc ROW<uv BIGINT, ss BIGINT>
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

  def createPrint(printlnSinkTbl: String): String = {
    s"""CREATE TABLE ${printlnSinkTbl} (
       |rowtime TIMESTAMP(3)
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
       |    'sink.buffer-flush.max-rows' = '100',
       |    'sink.buffer-flush.interval' = '100s'
       |
       |)""".stripMargin


  /**
   * 如果cdc的表作为时态表，那必须要有
   * 主键，和eventtime
   * @param tbl
   * @return
   */
  def cdcCreateTableSQL(tbl: String): String ={
    s"""CREATE TABLE $tbl (
       | id INT NOT NULL,
       | consignee STRING,
       | img_url STRING,
       | create_time TIMESTAMP(3),
       | proc_time AS PROCTIME(),
       | PRIMARY KEY(consignee) NOT ENFORCED,
       | WATERMARK FOR create_time AS create_time
       |) WITH (
       | 'connector' = 'mysql-cdc',
       | 'hostname' = 'localhost',
       | 'port' = '3306',
       | 'username' = 'root',
       | 'password' = '123456',
       | 'database-name' = 'TEST',
       | 'table-name' = 'order_info'
       |)""".stripMargin
  }
}
