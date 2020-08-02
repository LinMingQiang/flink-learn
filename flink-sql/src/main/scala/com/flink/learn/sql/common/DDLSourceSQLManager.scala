package com.flink.learn.sql.common

import com.flink.common.core.FlinkLearnPropertiesUtil

object DDLSourceSQLManager {

  /**
    * csv格式 的输入
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
                              |    id VARCHAR,
                              |    name VARCHAR,
                              |    age INT
                              |) WITH (
                              |    'connector.type' = 'kafka',
                              |    'connector.version' = '0.10',
                              |    'connector.topic' = '$topic',
                              |    'connector.startup-mode' = 'latest-offset',
                              |    'connector.properties.zookeeper.connect' = '$zk',
                              |    'connector.properties.bootstrap.servers' = '$broker',
                              |    'connector.properties.group.id' = '$groupID',
                              |    'update-mode' = 'append',
                              |    'format.type' = 'csv',
                              |    'format.field-delimiter' = '$delimiter',
                              |    'format.derive-schema' = 'true',
                              |    'format.ignore-parse-errors' = 'true'
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
       |    'connector.type' = 'kafka',
       |    'connector.version' = '0.10',
       |    'connector.topic' = '$topic',
       |    'connector.startup-mode' = 'latest-offset',
       |    'connector.properties.zookeeper.connect' = '$zk',
       |    'connector.properties.bootstrap.servers' = '$broker',
       |    'connector.properties.group.id' = '$groupID',
       |    'update-mode' = 'append',
       |    'format.type' = 'json',
       |    'format.derive-schema' = 'true'
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
       |    'connector.type' = 'kafka',
       |    'connector.version' = '0.10',
       |    'connector.topic' = '$topic',
       |    'connector.startup-mode' = 'latest-offset',
       |    'connector.properties.zookeeper.connect' = '$zk',
       |    'connector.properties.bootstrap.servers' = '$broker',
       |    'connector.properties.group.id' = '$groupID',
       |    'update-mode' = 'append',
       |    'format.type' = 'json',
       |    'format.derive-schema' = 'true'
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

def createCustomSinkTbl(printlnSinkTbl: String): String ={
  s"""CREATE TABLE ${printlnSinkTbl} (
     |topic VARCHAR,
     |ll BIGINT,
     |msg VARCHAR
     |) WITH (
     |'connector.type' = 'printsink',
     |'println.prefix'='>> : '
     |)""".stripMargin
}


  def createCustomPrintlnRetractSinkTbl(printlnSinkTbl: String): String ={
    s"""CREATE TABLE ${printlnSinkTbl} (
       |topic VARCHAR,
       |msg VARCHAR,
       |ll BIGINT
       |) WITH (
       |'connector.type' = 'printsink_retract',
       |'println.prefix'='>> : '
       |)""".stripMargin
  }

  def createCustomHbaseSinkTbl(hbasesinkTbl: String): String ={
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
