package com.streamtable.scala.test

import com.flink.common.core.FlinkLearnPropertiesUtil.BROKER
import com.flink.common.manager.TableSourceConnectorManager
import com.flink.learn.sql.common.{DataFormatUril, SchemaManager}
import com.flink.learn.test.common.FlinkStreamTableCommonSuit
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

class FlinkLearnStreamConnectEntry extends FlinkStreamTableCommonSuit {

  // {"id":"1","name":"211","age":5}
  /**
    *
    */
  test("createTestTbl") {
    // kafka source
    val kafkaConnector =
      TableSourceConnectorManager.kafkaConnector(BROKER, "test", "test", "latest")
    val jsonFormat = DataFormatUril.kafkaConnJsonFormat(kafkaConnector)
    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
    tableEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA)
      .inAppendMode()
      .createTemporaryTable("test")

    tableEnv.sqlQuery("select * from test")
      .toRetractStream[Row]
      .print()
    tableEnv.execute("")
  }

  /**
    *
    */
  test("createTest2Tbl") {
    // kafka source
    val kafkaConnector =
      TableSourceConnectorManager.kafkaConnector(BROKER, "test2", "test", "latest")
    val jsonFormat = DataFormatUril.kafkaConnJsonFormat(kafkaConnector)
    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
    tableEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA)
      .inAppendMode()
      .createTemporaryTable("test2")
  }

  /**
    *
    */
  test("streamJoin") {
    tableEnv
      .sqlQuery(s"""select a.*,b.* from test a join test2 b on a.id = b.id""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print()
    tableEnv.execute("FlinkLearnStreamConnectEntry")
  }

  /**
    *
    */
  test("streamCountDistinct") {
    // excute sql
    tableEnv
      .sqlQuery(s"""select a.*,b.* from test a join test2 b on a.id = b.id""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print()
    tableEnv.execute("FlinkLearnStreamConnectEntry")
  }
}
