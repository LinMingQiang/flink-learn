package com.flink.learn.sql.common

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, Json, Kafka, Schema}

object DataFormatUril {

  /**
    *
    * @param kafkaConnector
    * @return
    */
  def kafkaConnJsonFormat(kafkaConnector: Kafka): Json = {
    val jsonFormat = new Json()
      .deriveSchema()
      .failOnMissingField(false)
    jsonFormat
  }

  /**
    *
    * @param kafkaConnector
    */
  def kafkaConnCsvFormat(kafkaConnector: Kafka): Csv = {
    val csvFo = new Csv()
      .deriveSchema()
      .fieldDelimiter(',')
      .lineDelimiter("\n")
      .ignoreParseErrors()
    csvFo
  }
}
