package com.flink.learn.sql.common

import com.flink.common.core.FlinkLearnPropertiesUtil

object DDLSourceSQLManager {

  val createFromkafkasql = s"""CREATE TABLE reportTbl (
                              |    lon VARCHAR,
                              |    rideTime VARCHAR,
                              |    a VARCHAR,
                              |    s VARCHAR,
                              |    d VARCHAR,
                              |    f VARCHAR,
                              |    g VARCHAR,
                              |    h VARCHAR,
                              |    j VARCHAR
                              |) WITH (
                              |    'connector.type' = 'kafka',
                              |    'connector.version' = '0.10',
                              |    'connector.topic' = 'requestlog',
                              |    'connector.startup-mode' = 'earliest-offset', -- earliest-offset /  group-offsets
                              |    'connector.properties.0.key' = 'bootstrap.servers',
                              |    'connector.properties.0.value' = 'localhost:9092',
                              |    'update-mode' = 'append',
                              |    'format.type' = 'csv',
                              |    'format.field-delimiter' = '|',
                              |    'format.derive-schema' = 'true',
                              |    'format.ignore-parse-errors' = 'true'
                              |)""".stripMargin

  // -- earliest-offset /  group-offsets
  def createStreamFromKafka(topic: String, tableName: String): String = {
    s"""CREATE TABLE $tableName (
       |    id VARCHAR,
       |    name VARCHAR,
       |    age INT
       |) WITH (
       |    'connector.type' = 'kafka',
       |    'connector.version' = '0.10',
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

  def ddlKafkaWithWartermark(): Unit = {}

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
       |    'connector.type' = 'kafka',
       |    'connector.version' = '0.10',
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
//  def createStreamFromKafka(topic: String, tableName: String): String = {
//    s"""CREATE TABLE $tableName (
//       |    id VARCHAR,
//       |    name VARCHAR,
//       |    age INT
//       |) WITH (
//       |    'connector.type' = 'kafka',
//       |    'connector.version' = '0.10',
//       |    'connector.topic' = '$topic',
//       |    'connector.startup-mode' = 'latest-offset',
//       |    'connector.properties.zookeeper.connect' = 'localhost:2181',
//       |    'connector.properties.bootstrap.servers' = 'localhost:9092',
//       |    'connector.properties.group.id' = 'testGroup',
//       |    'update-mode' = 'append',
//       |    'format.type' = 'csv',
//       |    'format.field-delimiter' = ',',
//       |    'format.derive-schema' = 'true',
//       |    'format.ignore-parse-errors' = 'true'
//       |)""".stripMargin
//  }

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
  //  val createFromkafkasql = s"""-- source表
  //                              |CREATE TABLE user_log (
  //                              |    user_id VARCHAR,
  //                              |    item_id VARCHAR,
  //                              |    category_id VARCHAR,
  //                              |    behavior VARCHAR,
  //                              |    ts TIMESTAMP
  //                              |) WITH (
  //                              |    'connector.type' = 'kafka', -- 使用 kafka connector
  //                              |    'connector.version' = '0.10',  -- kafka 版本，universal 支持 0.11 以上的版本
  //                              |    'connector.topic' = 'kafka-flink-sql',  -- kafka topic
  //                              |    'connector.startup-mode' = 'earliest-offset', -- 从起始 offset 开始读取.optional: valid modes are "earliest-offset", "latest-offset", "group-offsets", or "specific-offsets"
  //                              |    'connector.properties.0.key' = 'bootstrap.servers',
  //                              |    'connector.properties.0.value' = 'localhost:9092',
  //                              |    'connector.properties.1.key' = 'group.id',
  //                              |    'connector.properties.1.value' = 'testGroup',
  //                              |    'update-mode' = 'append',
  //                              |    'format.type' = 'json',  -- 数据源格式为 json
  //                              |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
  //                              |)""".stripMargin

}
