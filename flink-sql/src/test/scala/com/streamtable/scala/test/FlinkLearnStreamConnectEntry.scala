package com.streamtable.scala.test

import com.flink.common.core.CaseClassManager.{Order, WC}
import com.flink.common.core.FlinkLearnPropertiesUtil.BROKER
import com.flink.common.manager.{SchemaManager, TableSourceConnectorManager}
import com.flink.learn.test.common.FlinkStreamTableCommonSuit
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

class FlinkLearnStreamConnectEntry extends FlinkStreamTableCommonSuit {

//  test("executeSql") {
//    val a = tableEnv.fromDataStream(
//      getKafkaDataStream("test", "localhost:9092", "latest"),
//      'topic,
//      'offset,
//      'msg)
//    tableEnv.createTemporaryView("test", a)
//    tableEnv.executeSql(
//      DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl(
//        "printlnSink_retract"))
//    tableEnv
//      .sqlQuery("select topic,msg,count(*) as ll from test group by topic,msg")
//      .insertInto("printlnSink_retract")
//    // 这里需要用tableEnv.execute("jobname"); 而不是 streamEnv.execute("jobname")
//    tableEnv.execute("jobname")
//  }
//
//
//  /**
//   * scala报 No operators defined in streaming topology. Cannot generate StreamGraph.
//   * 未解决
//   */
//  test("stream") {
//    val tableA = tableEnv.fromDataStream(
//      getKafkaDataStream("test", "localhost:9092", "latest"),
//      'topic,
//      'offset,
//      'msg)
//    tableEnv.createTemporaryView("test", tableA)
//    tableEnv
//      .toRetractStream[Row](
//        tableEnv.sqlQuery(
//          "select topic,msg,count(*) as ll from test group by topic,msg"))
//      .addSink(x => println(x))
//
//    streamEnv.execute("")
//  }


  /**
   *
   */
//  test("stream join") {
//    val tableA = tableEnv.fromDataStream(
//      getKafkaDataStream("test", "localhost:9092", "latest"),
//      'topic,
//      'offset,
//      'msg)
//    val tableB = tableEnv.fromDataStream(
//      getKafkaDataStream("test", "localhost:9092", "latest"),
//      'topic,
//      'offset,
//      'msg)
//    tableEnv.createTemporaryView("tableA", tableA)
//    tableEnv.createTemporaryView("tableB", tableB)
//    tableEnv.executeSql(
//      DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl(
//        "printlnSink_retract"))
//    //    tableEnv.getConfig
////      .setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
//    // sql 结果转为 streamDataset
//    val joinT = tableEnv
//      .sqlQuery(
//        "select a.topic,a.msg from tableA a join tableB b on a.msg=b.msg")
//    tableEnv.createTemporaryView("jointable", joinT)
//    tableEnv
//      .sqlQuery(
//        "select topic,msg,count(*) as ll from jointable group by topic,msg")
//      .insertInto("printlnSink_retract")
//    tableEnv.execute("")
//  }
  // {"id":"1","name":"211","age":5}
  /**
    *
    */
  test("createTestTbl") {
    // kafka source
    val kafkaConnector =
      TableSourceConnectorManager.kafkaConnector(BROKER,
                                                 "test",
                                                 "test",
                                                 "latest")
    val jsonFormat = ConnectorFormatDescriptorUtils.kafkaConnJsonFormat()
    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
    tableEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA)
      .inAppendMode()
      .createTemporaryTable("test")

    tableEnv
      .sqlQuery("select * from test")
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
      TableSourceConnectorManager.kafkaConnector(BROKER,
                                                 "test2",
                                                 "test",
                                                 "latest")
    val jsonFormat = ConnectorFormatDescriptorUtils.kafkaConnJsonFormat()
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

  test("wordCountTest") {
    // val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val input =
      bEnv.fromCollection(Array(WC("hello", 1), WC("hello", 1), WC("ciao", 1)))
    // register the DataSet as table "WordCount"
    batchEnv.createTemporaryView("WordCount", input, 'word, 'frequency)
    // run a SQL query on the Table and retrieve the result as a new Table
    val table =
      batchEnv.sqlQuery(
        "SELECT word, SUM(frequency) FROM WordCount GROUP BY word")
    // table.toDataSet[WC].print()
    table.toDataSet[Row].print()
  }
}
