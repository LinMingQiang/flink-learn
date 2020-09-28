package com.streamtable.scala.test

import com.flink.common.core.CaseClassManager.WC
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.java.pojo.WordCountPoJo
import com.flink.learn.test.common.{FlinkJavaStreamTableTestBase, FlinkStreamTableCommonSuit}
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, _}
import org.apache.flink.types.Row

class FlinkLearnDataSetSQLEntry extends FlinkStreamTableCommonSuit{
  def testFactory(): Unit = {
//    val streamEnv = FlinkEvnBuilder.buildStreamingEnv(
//      FlinkLearnPropertiesUtil.param,
//      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
//      FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL)
//    val tableEnv = FlinkEvnBuilder.buildStreamTableEnv(streamEnv,
//                                                       Time.minutes(1),
//                                                       Time.minutes(6))
//    val a = tableEnv.fromDataStream(
//      streamEnv.addSource(
//        FlinkJavaStreamTableTestBase
//          .getKafkaSource("test", "localhost:9092", "latest")),
//      "topic,offset,msg").renameColumns("offset as ll"); // offset是关键字
//    tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomSinkTbl("printlnSinkTbl"))
//
//    tableEnv.insertInto("printlnSinkTbl", a.select("topic,ll,msg"))
//
//
//    tableEnv.execute("eee")
  }

  test("wordCountTest"){
     // val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val input =
      bEnv.fromCollection(Array(WC("hello", 1), WC("hello", 1), WC("ciao", 1)))
    // register the DataSet as table "WordCount"
    batchEnv.createTemporaryView("WordCount", input, 'word, 'frequency)
    // run a SQL query on the Table and retrieve the result as a new Table
    val table =
      batchEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")
    // table.toDataSet[WC].print()
    table.toDataSet[Row].print()
  }
}
