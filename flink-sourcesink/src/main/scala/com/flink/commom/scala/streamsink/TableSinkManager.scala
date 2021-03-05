package com.flink.commom.scala.streamsink

object TableSinkManager {

//  def registerCsvTableSink(tEnv: StreamTableEnvironment,
//                           tblName: String,
//                           col: Array[String],
//                           colType: Array[TypeInformation[_]],
//                           path: String,
//                           fieldDelim: String,
//                           fileNum: Int,
//                           writeM: WriteMode): Unit = {
//    ;
//    val sink = new CsvTableSink(
//      path, // output path
//      fieldDelim, // optional: delimit files by '|'
//      fileNum, // optional: write to a single file
//      writeM
//    )
//
//    tEnv.registerTableSink(tblName,
//                           // specify table schema
//                           col,
//                           colType,
//                           sink)
//  }
//  def registerJavaCsvTableSink(
//      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment,
//      tblName: String,
//      col: Array[String],
//      colType: Array[TypeInformation[_]],
//      path: String,
//      fieldDelim: String,
//      fileNum: Int,
//      writeM: WriteMode): Unit = {
//    ;
//    val sink = new CsvTableSink(
//      path, // output path
//      fieldDelim, // optional: delimit files by '|'
//      fileNum, // optional: write to a single file
//      writeM
//    )
//    tEnv.registerTableSink(tblName,
//                           // specify table schema
//                           col,
//                           colType,
//                           sink)
//  }
//
//  /**
//    * 已过期
//    * @param tEnv
//    */
//  def registAppendStreamTableSink(
//      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment): Unit = {
//    tEnv.registerTableSink(
//      "test",
//      Array("topic", "offset", "msg"),
//      Array[TypeInformation[_]](Types.STRING, Types.LONG, Types.STRING),
//      new PrintlnAppendStreamTableSink)
//  }

//  /**
//    * connect的方式也过期了，建议是用ddl的方式建立
//    * @param tEnv
//    * @param tablename
//    */
//  def connctKafkaSink(
//      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment,
//      tablename: String): Unit = {
//    val kafkaConnector =
//      TableSourceConnectorManager.kafkaConnector("localhost:9092",
//                                         tablename,
//                                         "test",
//                                         "latest")
//    val jsonFormat = ConnectorFormatDescriptorUtils.kafkaConnJsonFormat()
//    // lazy val csvFormat = DataFormatUril.kafkaConnCsvFormat(kafkaConnector)
//    tEnv
//      .connect(kafkaConnector)
//      .withFormat(jsonFormat)
//      .withSchema(SchemaManager.KAFKA_SCHEMA)
//      .inAppendMode()
//      .createTemporaryTable(tablename)
//  }
//
//  /**
//    * csv
//    * @param tEnv
//    * @param tablename
//    */
//  def connectFileSystemSink(
//      tEnv: org.apache.flink.table.api.bridge.java.StreamTableEnvironment,
//      tablename: String): Unit = {
//    tEnv
//      .connect(new FileSystem()
//        .path("file:///Users/eminem/workspace/flink/flink-learn/checkpoint/filesink/csv"))
//      .withFormat(new Csv()
//        .fieldDelimiter(','))
//      .withSchema(SchemaManager.KAFKA_SCHEMA)
//      .createTemporaryTable(tablename)
//
//  }
}
