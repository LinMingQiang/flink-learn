package com.flink.learn.sql.common

object SQLManager {

  val createFromkafkasql = s"""CREATE TABLE ssp_sdk_report (
                              |    lon VARCHAR,
                              |    rideTime VARCHAR,
                              |    a VARCHAR,
                              |    s VARCHAR,
                              |    d VARCHAR,
                              |    f VARCHAR,
                              |    g VARCHAR,
                              |    h VARCHAR,
                              |    j VARCHAR,
                              |    k VARCHAR,
                              |    i VARCHAR,
                              |    l VARCHAR,
                              |     q VARCHAR,
                              |      w VARCHAR,
                              |       e VARCHAR,
                              |        r VARCHAR,
                              |         t VARCHAR,
                              |          y VARCHAR,
                              |           zz VARCHAR,
                              |            la VARCHAR,
                              |             ss VARCHAR,
                              |             dd VARCHAR,
                              |              ff VARCHAR,
                              |               gg VARCHAR,
                              |                dada VARCHAR,
                              |                dadaa VARCHAR,
                              |                er VARCHAR,
                              |                aa VARCHAR,
                              |                nn VARCHAR,
                              |                mm VARCHAR
                              |
                              |) WITH (
                              |    'connector.type' = 'kafka',
                              |    'connector.version' = '0.10',
                              |    'connector.topic' = 'impresslog',
                              |    'connector.startup-mode' = 'latest-offset', -- earliest-offset
                              |    'connector.properties.0.key' = 'bootstrap.servers',
                              |    'connector.properties.0.value' = 'localhost:9092',
                              |    'update-mode' = 'append',
                              |    'format.type' = 'csv',
                              |    'format.field-delimiter' = '|',
                              |    'format.derive-schema' = 'true',
                              |    'format.ignore-parse-errors' = 'true'
                              |)""".stripMargin

  val createFromMysql = s"""CREATE TABLE ssp_sdk_report (
                           |    bid_req_num BIGINT,
                           |    md_key VARCHAR
                           |) WITH (
                           |    'connector.type' = 'jdbc',
                           |    'connector.url' = 'jdbc:mysql://ip:3306/databases',
                           |    'connector.table' = 'table',
                           |    'connector.username' = 'root',
                           |    'connector.password' = '123456',
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
  //                              |    'connector.properties.0.value' = '10.21.33.28:9092,10.21.33.29:9092,10.21.131.11:9092',
  //                              |    'connector.properties.1.key' = 'group.id',
  //                              |    'connector.properties.1.value' = 'testGroup',
  //                              |    'update-mode' = 'append',
  //                              |    'format.type' = 'json',  -- 数据源格式为 json
  //                              |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
  //                              |)""".stripMargin


}
