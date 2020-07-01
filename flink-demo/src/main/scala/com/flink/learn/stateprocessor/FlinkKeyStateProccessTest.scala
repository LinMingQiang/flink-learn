package com.flink.learn.stateprocessor
import com.flink.learn.bean.CaseClassUtil._
import com.flink.learn.reader.{WordCounKeyreader}
import com.flink.learn.trans.AccountKeyedStateBootstrapFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.{ExecutionEnvironment}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.state.api.{
  BootstrapTransformation,
  ExistingSavepoint,
  OperatorTransformation,
  Savepoint
}

/**
  * keyBy("word") 和 keyBy(x => x.word) 出来的序列化是不一样的，后者在读取的时候用 String ，前者用读不出来.
  * 所以统一以后用 keyBy(x => x.word)
  *
  */
object FlinkKeyStateProccessTest {
  var uid = "wordcountUID"
  val ckpPath = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  val srcCkpPath = ckpPath + "/SocketScalaWordcountTest/202007011707/2e6c99d63113b9e7fa28cc3af57ca318/chk-1"
  val newPath = ckpPath + "/scalatanssavepoint"
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  def main(args: Array[String]): Unit = {
    val existSp =
      Savepoint.load(bEnv, srcCkpPath, new RocksDBStateBackend(ckpPath))
    readKeyState(existSp, uid).print
    // transKeystateAndWritebak(existSp)
    // bEnv.execute("FlinkKeyStateProccessTest") // print 会自动调用execute ，所以注释掉，否则报错
  }

  /**
    * 读取 state数据
    * @param existSp
    * @param uid
    * @return
    */
  def readKeyState(existSp: ExistingSavepoint, uid: String) = {
    existSp
      .readKeyedState(
        uid,
        new WordCounKeyreader("wordcountState")
      )
  }

  /**
    * 转换数据并写回savepoint
    * @param existSp
    */
  def transKeystateAndWritebak(existSp: ExistingSavepoint): Unit = {
    val oldState = readKeyState(existSp, uid)
    oldState.print()
    val transformation: BootstrapTransformation[TransWordCount] =
      OperatorTransformation
        .bootstrapWith(oldState)
        .keyBy(new KeySelector[TransWordCount, String] {
          override def getKey(value: TransWordCount): String = value.word
        })
        //.keyBy("word") // 这个在KeyedStateBootstrapFunction 只能用K = Tuple，但是写回去就读不出来了
        .transform(new AccountKeyedStateBootstrapFunction)

    existSp
      .removeOperator(uid)
      .withOperator(uid, transformation)
      .write(newPath)

    bEnv.execute("FlinkKeyStateProccessTest") // print 会自动调用execute ，所以注释掉，否则报错
  }
}
