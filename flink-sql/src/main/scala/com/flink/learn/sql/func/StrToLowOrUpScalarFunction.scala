package com.flink.learn.sql.func

import org.apache.flink.table.functions.{FunctionContext, ScalarFunction}

object StrToLowOrUpScalarFunction extends ScalarFunction {
  var hashcode_factor: String = null
  override def open(context: FunctionContext): Unit = {
    // access "hashcode_factor" parameter
    // "12" would be the default value if parameter does not exist
    hashcode_factor = context.getJobParameter("loworupper", "UP")
  }

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
