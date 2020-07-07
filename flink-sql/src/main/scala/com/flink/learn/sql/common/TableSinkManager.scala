package com.flink.learn.sql.common

import java.lang

import com.flink.common.java.sink.PrintlnAppendStreamTableSink
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem}
import org.apache.flink.table.sinks.{AppendStreamTableSink, CsvTableSink, RetractStreamTableSink, TableSink}
import org.apache.flink.types.Row

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
                           Array[TypeInformation[_]](Types.LONG, Types.STRING),
                           sink)
  }

  def registAppendStreamTableSink(
      tEnv: org.apache.flink.table.api.java.StreamTableEnvironment): Unit = {
    tEnv.registerTableSink(
      "test",
      Array("topic", "offset", "msg"),
      Array[TypeInformation[_]](Types.STRING, Types.LONG, Types.STRING),
      new PrintlnAppendStreamTableSink())
  }

  def connctKafkaSink(tEnv: org.apache.flink.table.api.java.StreamTableEnvironment,  tablename: String): Unit ={
    val kafkaConnector =
      SqlConnectorManager.kafkaConnector("localhost:9092", tablename, "test", "latest")
    val jsonFormat = DataFormatUril.kafkaConnJsonFormat(kafkaConnector)
    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
    tEnv
      .connect(kafkaConnector)
      .withFormat(jsonFormat)
      .withSchema(SchemaManager.KAFKA_SCHEMA)
      .inAppendMode()
      .createTemporaryTable(tablename)
  }

  def connectFileSystemSink(tEnv: org.apache.flink.table.api.java.StreamTableEnvironment,  tablename: String): Unit ={
    tEnv.connect(
      new FileSystem()
      .path("file:///Users/eminem/workspace/flink/flink-learn/checkpoint/filesink/csv"))
      .withFormat(new Csv()
        .fieldDelimiter(',')         // optional: field delimiter character (',' by default)
        .lineDelimiter("\r\n")       // optional: line delimiter ("\n" by default;
        //   otherwise "\r", "\r\n", or "" are allowed)
        //   cannot define a quote character and disabled quote character at the same time
        .quoteCharacter('\'')        // optional: quote character for enclosing field values ('"' by default)
        .allowComments()             // optional: ignores comment lines that start with '#' (disabled by default);
        //   if enabled, make sure to also ignore parse errors to allow empty rows
        .ignoreParseErrors()         // optional: skip fields and rows with parse errors instead of failing;
        //   fields are set to null in case of errors
        .arrayElementDelimiter("|")  // optional: the array element delimiter string for separating
        //   array and row element values (";" by default)
        .escapeCharacter('\\')       // optional: escape character for escaping values (disabled by default)
        .nullLiteral("n/a")   )
      .withSchema(SchemaManager.KAFKA_SCHEMA)
      .createTemporaryTable(tablename)

  }
}
