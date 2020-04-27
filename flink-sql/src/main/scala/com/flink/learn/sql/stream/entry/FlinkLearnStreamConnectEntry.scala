package com.flink.learn.sql.stream.entry

import com.flink.common.core.{FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Schema}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
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
    val env = FlinkEvnBuilder.buildStreamingEnv(
      FlinkLearnPropertiesUtil.param,
      FlinkLearnPropertiesUtil.CHECKPOINT_PATH,
      10000) // 1 min
    val sett =
      EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val streamTableEnv = StreamTableEnvironment.create(env, sett)
    val kafkaConnector = new Kafka()
      .version("0.10")
      .topic("test")
      .property("bootstrap.servers", "localhost:9092")
      .property("group.id", "test2")
      .property("zookeeper.connect", "localhost:2181")
      .startFromLatest()
    // kafkaConnector.startFromEarliest()
    csvFormat(streamTableEnv, kafkaConnector)
    // jsonFormat(streamTableEnv, kafkaConnector)
    streamTableEnv.execute("sql test")

  }

  /**
    *
    * @param streamTableEnv
    * @param kafkaConnector
    */
  def csvFormat(streamTableEnv: StreamTableEnvironment,
                kafkaConnector: Kafka): Unit = {
    val csvFo = new Csv()
      .deriveSchema()
      .fieldDelimiter(',')
      .lineDelimiter("\n")
      .ignoreParseErrors()
    //默认加一个PROCESSING_TIME时间属性字段
    val schema = new Schema()
      .field("id", Types.STRING)
      .field("name", Types.STRING)
      .field("age", Types.INT)
    streamTableEnv
      .connect(kafkaConnector)
      .withFormat(csvFo)
      .withSchema(schema)
      .inAppendMode()
      .registerTableSource("test")
//    streamTableEnv
//      .sqlQuery("select * from test")
//      .toAppendStream[Row]
//      .print()
    streamTableEnv
      .sqlQuery(s"""select id,count(distinct age) from test group by id""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print()
  }

  /**
    *
    * @param streamTableEnv
    * @param kafkaConnector
    */
  def jsonFormat(streamTableEnv: StreamTableEnvironment,
                 kafkaConnector: Kafka): Unit = {
    val jsonFormat = new Json()
      .deriveSchema()
      .failOnMissingField(false)

    val schema = new Schema()
      .field("id", Types.STRING)
      .field("name", Types.STRING)
      .field("age", Types.INT)
    streamTableEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(schema)
      .inAppendMode()
      .registerTableSource("test")
    streamTableEnv
      .sqlQuery("select * from test")
      .toAppendStream[Row]
      .print()

  }
}
