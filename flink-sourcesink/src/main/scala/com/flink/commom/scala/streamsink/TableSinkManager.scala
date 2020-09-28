package com.flink.commom.scala.streamsink

import com.flink.common.java.tablesink.PrintlnAppendStreamTableSink
import com.flink.common.manager.{SchemaManager, TableSourceConnectorManager}
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem}
import org.apache.flink.table.sinks.CsvTableSink

object TableSinkManager {

  /**
    *
    * @param tEnv
    * @param tblName
    * @param col     Array[String]("bid_req_num", "md_key")
    * @param colType Array[TypeInformation[_]](Types.LONG, Types.STRING)
    * @param path
    * @param fieldDelim
    * @param fileNum
    * @param writeM
    */
  def registerCsvTableSink(tEnv: StreamTableEnvironment,
                           tblName: String,
                           col: Array[String],
                           colType: Array[TypeInformation[_]],
                           path: String,
                           fieldDelim: String,
                           fileNum: Int,
                           writeM: WriteMode): Unit = {
    ;
    val sink = new CsvTableSink(
      path, // output path
      fieldDelim, // optional: delimit files by '|'
      fileNum, // optional: write to a single file
      writeM
    )
    tEnv.registerTableSink(tblName,
                           // specify table schema
                           col,
                           colType,
                           sink)
  }

  /**
    *
    * @param tEnv
    * @param tblName
    * @param col
    * @param colType
    * @param path
    * @param fieldDelim
    * @param fileNum
    * @param writeM
    */
  def registerJavaCsvTableSink(
      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment,
      tblName: String,
      col: Array[String],
      colType: Array[TypeInformation[_]],
      path: String,
      fieldDelim: String,
      fileNum: Int,
      writeM: WriteMode): Unit = {
    ;
    val sink = new CsvTableSink(
      path, // output path
      fieldDelim, // optional: delimit files by '|'
      fileNum, // optional: write to a single file
      writeM
    )
    tEnv.registerTableSink(tblName,
                           // specify table schema
                           col,
                           colType,
                           sink)
  }

  /**
    * 已过期
    * @param tEnv
    */
  def registAppendStreamTableSink(
      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment): Unit = {
    tEnv.registerTableSink(
      "test",
      Array("topic", "offset", "msg"),
      Array[TypeInformation[_]](Types.STRING, Types.LONG, Types.STRING),
      new PrintlnAppendStreamTableSink)
  }

  /**
    * kafka
    * @param tEnv
    * @param tablename
    */
  def connctKafkaSink(
      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment,
      tablename: String): Unit = {
    val kafkaConnector =
      TableSourceConnectorManager.kafkaConnector("localhost:9092",
                                         tablename,
                                         "test",
                                         "latest")
    val jsonFormat = ConnectorFormatDescriptorUtils.kafkaConnJsonFormat()
    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
    tEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(SchemaManager.KAFKA_SCHEMA)
      .inAppendMode()
      .createTemporaryTable(tablename)
  }

  /**
    * csv
    * @param tEnv
    * @param tablename
    */
  def connectFileSystemSink(
      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment,
      tablename: String): Unit = {
    tEnv
      .connect(new FileSystem()
        .path("file:///Users/eminem/workspace/flink/flink-learn/checkpoint/filesink/csv"))
      .withFormat(new Csv()
        .fieldDelimiter(','))
      .withSchema(SchemaManager.KAFKA_SCHEMA)
      .createTemporaryTable(tablename)

  }
}
