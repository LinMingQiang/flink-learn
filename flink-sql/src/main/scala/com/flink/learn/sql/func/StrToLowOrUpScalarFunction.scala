package com.flink.learn.sql.func

import org.apache.commons.lang3.time.DateFormatUtils
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}
import org.apache.flink.types.Row

object StrToLowOrUpScalarFunction extends ScalarFunction {
  var hashcode_factor: String = null

  override def open(context: FunctionContext): Unit = {
    // access "hashcode_factor" parameter
    // "12" would be the default value if parameter does not exist
    hashcode_factor = context.getJobParameter("loworupper", "UP")
  }

  @DataTypeHint("ROW<s STRING>")
  def eval(tsamp: Long, format: String): Row =
    Row.of(DateFormatUtils.format(tsamp, format))

  /**
    *
    * @param s
    * @return
    */
  def eval(s: String): String = {
    hashcode_factor match {
      case "UP"  => s.toUpperCase
      case "LOW" => s.toLowerCase
      case _     => s.toUpperCase
    }

  }
}
