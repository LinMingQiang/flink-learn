package com.flink.learn.state.processor.entry
import java.util.Date

import com.flink.learn.reader.WordCounScalaKeyreader
import com.flink.learn.trans.AccountKeyedStateBootstrapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.state.api.{
  ExistingSavepoint,
  OperatorTransformation,
  Savepoint
}
object FlinkKeyStateProccessTest {
  var uid = "wordcountUID"
  val path = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  val savp = "file:///Users/eminem/workspace/flink/flink-learn/savepoint"
  val sourcePath = savp + "/savepoint-840b62-50c952b06168"
  val newPath = savp + "/" + new Date().getTime
  var date = "/1579158381789"
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  def main(args: Array[String]): Unit = {

    val existSp =
      Savepoint.load(bEnv, sourcePath, new RocksDBStateBackend(path))
    // readKeyState(existSp, uid).print
    transKeystateAndWritebak(existSp, newPath)
    bEnv.execute("sss")
  }

  /**
    *
    * @param existSp
    * @param uid
    * @return
    */
  def readKeyState(existSp: ExistingSavepoint, uid: String) = {
    existSp
      .readKeyedState(
        uid,
        new WordCounScalaKeyreader("wordcountState")
      )
  }

  /**
    *
    * @param existSp
    * @param newPath
    */
  def transKeystateAndWritebak(existSp: ExistingSavepoint,
                               newPath: String): Unit = {
    val oldState = readKeyState(existSp, uid)
    val transformation = OperatorTransformation
      .bootstrapWith(oldState)
      .keyBy("word")
      .transform(new AccountKeyedStateBootstrapFunction)
    existSp
      .removeOperator(uid)
      .withOperator(uid, transformation)
      .write(newPath)

  }
}
