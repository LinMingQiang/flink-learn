package com.flink.learn.sql.common

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.descriptors.Schema

object SchemaManager {
  //默认加一个PROCESSING_TIME时间属性字段
  val ID_NAME_AGE_SCHEMA = new Schema()
    .field("id", Types.STRING)
    .field("name", Types.STRING)
    .field("age", Types.INT)

  val KAFKA_SCHEMA = new Schema()
    .field("topic", Types.STRING)
    .field("offset", Types.LONG)
    .field("msg", Types.STRING)
}
