package com.flink.learn.sql.func

import org.apache.flink.table.functions.TableFunction

object DdlTableFunction {
  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String): Unit = {
      // use collect(...) to emit a row.
      str.split(separator).foreach(x => collect((x, x.length)))
    }
  }
}
