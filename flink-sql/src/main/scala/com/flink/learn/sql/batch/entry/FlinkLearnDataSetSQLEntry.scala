package com.flink.learn.sql.batch.entry

import com.flink.common.core.FlinkLearnPropertiesUtil
import com.flink.common.java.core.FlinkEvnBuilder
import com.flink.learn.sql.common.DDLSourceSQLManager
import com.flink.learn.test.common.FlinkStreamTableTestBase
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object FlinkLearnDataSetSQLEntry {
  def main(args: Array[String]): Unit = {

    testFactory

  }


  def testFactory(): Unit = {
    val streamEnv = FlinkEvnBuilder.buildStreamingEnv(
      FlinkLearnPropertiesUtil.param,
      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
      FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL)
    val tableEnv = FlinkEvnBuilder.buildStreamTableEnv(streamEnv,
                                                       Time.minutes(1),
                                                       Time.minutes(6))

    val a = tableEnv.fromDataStream(
      streamEnv.addSource(
        FlinkStreamTableTestBase
          .getKafkaSource("test", "localhost:9092", "latest")),
      "topic,offset,msg")
    a.printSchema()
    tableEnv.sqlUpdate(
      DDLSourceSQLManager.createCustomSinkTbl("printlnSinkTbl"))
    tableEnv.from("printlnSinkTbl").printSchema()
    tableEnv.insertInto("printlnSinkTbl", a)
    // sink1 : 转 stream后sink
    tableEnv.toAppendStream(a, classOf[Row]).print
    tableEnv.execute("eee")
  }

  /**
    *
    * @param env
    * @param tEnv
    */
  def wordCount(env: ExecutionEnvironment,
                tEnv: BatchTableEnvironment): Unit = {
    // val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    val input =
      env.fromCollection(Array(WC("hello", 1), WC("hello", 1), WC("ciao", 1)))
    // register the DataSet as table "WordCount"
    tEnv.registerDataSet("WordCount", input, 'word, 'frequency)
    // run a SQL query on the Table and retrieve the result as a new Table
    val table =
      tEnv.sqlQuery("SELECT word, SUM(frequency) FROM WordCount GROUP BY word")
    // table.toDataSet[WC].print()
    table.toDataSet[Row].print()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class WC(word: String, frequency: Long)

}
