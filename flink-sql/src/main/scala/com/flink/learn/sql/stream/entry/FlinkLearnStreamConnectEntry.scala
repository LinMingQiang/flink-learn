package com.flink.learn.sql.stream.entry

import com.flink.common.core.FlinkLearnPropertiesUtil.BROKER
import com.flink.common.core.{FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.learn.sql.common.{
  DataFormatUril,
  SchemaManager,
  SqlConnectorManager
}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.{EnvironmentSettings, StreamQueryConfig}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Schema}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import FlinkLearnPropertiesUtil._

object FlinkLearnStreamConnectEntry {

  /**
    * 使用api的方式连接source
    * @param args
    */
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(
      "/Users/eminem/workspace/flink/flink-learn/dist/conf/application.properties",
      "test")
    // val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamTableEnv = FlinkEvnBuilder.buildStreamTableEnv(param,
                                                             CHECKPOINT_PATH,
                                                             10000) // 1 min
    createTestTbl(streamTableEnv)
    createTest2Tbl(streamTableEnv)



    streamTableEnv.execute("FlinkLearnStreamConnectEntry")
  }
  // {"id":"1","name":"211","age":5}

  /**
    *
    * @param streamTableEnv
    */
  def createTestTbl(streamTableEnv: StreamTableEnvironment): Unit = {
    // kafka source
    val kafkaConnector =
      SqlConnectorManager.kafkaConnector(BROKER, "test", "test", "latest")
    val jsonFormat = DataFormatUril.kafkaConnJsonFormat(kafkaConnector)
    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
    streamTableEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA)
      .inAppendMode()
      .createTemporaryTable("test")
  }

  /**
    *
    * @param streamTableEnv
    */
  def createTest2Tbl(streamTableEnv: StreamTableEnvironment): Unit = {
    // kafka source
    val kafkaConnector =
      SqlConnectorManager.kafkaConnector(BROKER, "test2", "test", "latest")
    val jsonFormat = DataFormatUril.kafkaConnJsonFormat(kafkaConnector)
    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
    streamTableEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA)
      .inAppendMode()
      .createTemporaryTable("test2")
  }

  /**
    *
    * @param streamTableEnv
    */
  def streamJoin(streamTableEnv: StreamTableEnvironment): Unit = {
    streamTableEnv
      .sqlQuery(s"""select a.*,b.* from test a join test2 b on a.id = b.id""")
      .toRetractStream[Row](
        new StreamQueryConfig(Time.minutes(1).toMilliseconds,
                              Time.minutes(6).toMilliseconds))
      .filter(_._1)
      .map(_._2)
      .print()
  }

  /**
    *
    * @param streamTableEnv
    */
  def streamCountDistinct(streamTableEnv: StreamTableEnvironment): Unit = {
    // excute sql
    streamTableEnv
      .sqlQuery(s"""select a.*,b.* from test a join test2 b on a.id = b.id""")
      .toRetractStream[Row](
        new StreamQueryConfig(Time.minutes(1).toMilliseconds,
                              Time.minutes(6).toMilliseconds))
      .filter(_._1)
      .map(_._2)
      .print()
  }
}
