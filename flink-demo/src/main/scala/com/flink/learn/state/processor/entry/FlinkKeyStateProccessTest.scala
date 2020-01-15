package com.flink.learn.state.processor.entry
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.state.api.{
  ExistingSavepoint,
  OperatorTransformation,
  Savepoint
}
object FlinkKeyStateProccessTest {
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  def main(args: Array[String]): Unit = {
    val path =
      "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
    val savp =
      "file:///Users/eminem/workspace/flink/flink-learn/savepoint/savepoint-840b62-50c952b06168"
    val existSp = Savepoint.load(bEnv, savp, new RocksDBStateBackend(path))
    // readKeyState(existSp, "wordcountUID").print
    transKeystateAndWritebak(
      existSp,
      "file:///Users/eminem/workspace/flink/flink-learn/savepoint")
    // bEnv.execute("sss")
  }

  def readKeyState(existSp: ExistingSavepoint, uid: String) = {
    existSp
      .readKeyedState(
        uid,
        new WordCountKeyreader("wordcountState")
      )
  }

  def transKeystateAndWritebak(existSp: ExistingSavepoint,
                               newPath: String): Unit = {
    val oldState = readKeyState(existSp, "wordcountUID")
    // val oldState = bEnv.fromCollection(List(Wordcount("a" ,1L, 1L)).asJava)
    val transformation = OperatorTransformation
      .bootstrapWith(oldState)
      .keyBy(1)
      .transform(new AccountKeyedStateBootstrapFunction)
    existSp
      .withOperator("wordcountUID", transformation)
      .write(newPath)

  }
}
