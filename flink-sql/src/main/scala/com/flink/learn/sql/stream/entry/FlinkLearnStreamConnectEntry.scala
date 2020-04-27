package com.flink.learn.sql.stream.entry

import com.flink.common.core.{FlinkEvnBuilder, FlinkLearnPropertiesUtil}
import com.flink.learn.sql.common.{
  DataFormatUril,
  SchemaManager,
  SqlConnectorManager
}
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
    import FlinkLearnPropertiesUtil._
    val streamTableEnv = FlinkEvnBuilder.buildStreamTableEnv(param,
                                                             CHECKPOINT_PATH,
                                                             10000) // 1 min
    // kafka source
    val kafkaConnector =
      SqlConnectorManager.kafkaConnector(BROKER, "test", "test", "latest")
    streamTableEnv
      .connect(kafkaConnector)
      .withFormat(DataFormatUril.kafkaConnJsonFormat(kafkaConnector))
      .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA)
      .inAppendMode()
      .registerTableSource("test")
   // excute sql
    streamTableEnv
      .sqlQuery(s"""select id,count(distinct name) from test group by id""")
      .toRetractStream[Row]
      .filter(_._1)
      .map(_._2)
      .print()
    streamTableEnv.execute("FlinkLearnStreamConnectEntry")
  }
  // {"id":"2","name":"6","age":5}
}
