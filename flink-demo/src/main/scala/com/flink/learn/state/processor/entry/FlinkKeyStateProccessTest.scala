package com.flink.learn.state.processor.entry
import java.util.Date

import com.flink.learn.bean.CaseClassUtil.TransWordCount
import com.flink.learn.bean.TranWordCountPoJo
import com.flink.learn.reader.{
  TranWordCountKeyreader,
  TranWordCountPoJoKeyreader,
  WordCounKeyreader
}
import com.flink.learn.trans.AccountKeyedStateBootstrapFunction
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.state.api.{
  ExistingSavepoint,
  OperatorTransformation,
  Savepoint
}
import org.apache.flink.api.scala._

/**
  * 遇到 ： The new state serializer cannot be incompatible.
  * 是因为state存储的是java的序列化类，在readkeystate的时候需要用java的pojo来读取，不能用scala的case class
  * java 可以使用scala的 case class ，但是scala不能使用java的 pojo
  */
// 还是用java写吧，读可以读scala的caseclass出来，但是写回去，如果是用caseclass写回去，就读不出来了。
// java 的 pojo 就没问题。
object FlinkKeyStateProccessTest {
  var uid = "wordcountUID"
  val ckpPath = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  val srcCkpPath = ckpPath + "/SocketScalaWordcountTest/202006301606/48b300d2fd5b88b6f42116624ce619bd/chk-5"
  val newPath = ckpPath + "/tanssavepoint"
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  def main(args: Array[String]): Unit = {

//    val existSp =  Savepoint.load(bEnv, srcCkpPath, new RocksDBStateBackend(ckpPath))
//     readKeyState(existSp, uid).print

//    val existSp =  Savepoint.load(bEnv, srcCkpPath, new RocksDBStateBackend(ckpPath))
//     transKeystateAndWritebak(existSp, newPath)

    // 当写caseclass 回去后就 读不出来，报 The new state serializer cannot be incompatible.
//    val existSp =
//      Savepoint.load(bEnv, newPath, new RocksDBStateBackend(ckpPath))
//    readTransKeyState(existSp, uid).print()

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
    * 读取 state数据
    * @param existSp
    * @param uid
    * @return
    */
  def readTransKeyState(existSp: ExistingSavepoint, uid: String) = {
    existSp
      .readKeyedState(
        uid,
        new TranWordCountPoJoKeyreader("wordcountState")
      )
  }

  /**
    * 转换数据并写回savepoint
    * @param existSp
    * @param newPath
    */
  def transKeystateAndWritebak(existSp: ExistingSavepoint,
                               newPath: String): Unit = {
    val oldState = readKeyState(existSp, uid)
//      .map(
//      new MapFunction[TransWordCount, TranWordCountPoJo] {
//        override def map(value: TransWordCount): TranWordCountPoJo = {
//          val r = new TranWordCountPoJo()
//          r.word = value.word
//          r.count = value.count
//          r.timestamp = value.timestamp
//          r
//        }
//      })
    oldState.print()
    val transformation = OperatorTransformation
      .bootstrapWith(oldState)
      .keyBy("word")
      .transform(new AccountKeyedStateBootstrapFunction)

    existSp
      .removeOperator(uid)
      .withOperator(uid, transformation)
      .write(newPath)

    bEnv.execute("FlinkKeyStateProccessTest") // print 会自动调用execute ，所以注释掉，否则报错
  }
}
