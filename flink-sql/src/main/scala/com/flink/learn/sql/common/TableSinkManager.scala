package com.flink.learn.sql.common

import java.lang

import com.flink.common.java.sink.{PrintlnAppendStreamTableSink}
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.scala.StreamTableEnvironment
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
}
