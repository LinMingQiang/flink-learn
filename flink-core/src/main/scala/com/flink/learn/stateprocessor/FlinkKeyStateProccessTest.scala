package com.flink.learn.stateprocessor
import com.flink.learn.bean.CaseClassUtil._
import com.flink.learn.bean.WordCountGroupByKey
import com.flink.learn.reader.{WordCounKeyreader, WordCounPojoKeyreader, WordCounTuple2Keyreader}
import com.flink.learn.trans.{AccountKeyedStateBootstrapFunction, AccountPojoKeyedStateBootstrapFunction, AccountTuple2KeyedStateBootstrapFunction}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.state.api.{BootstrapTransformation, ExistingSavepoint, OperatorTransformation, Savepoint}
import org.apache.flink.api.java.tuple.Tuple2
/**
  * 1: keyBy("word") 和 keyBy(x => x.word) 出来的序列化是不一样的，后者在读取的时候用 String ，前者用的Tuple.
  * 所以统一以后用 keyBy(x => x.word)
  * 2: 使用复合的 keyby类型就不行，报错。。需要使用 java 的 tuple2
  */
object FlinkKeyStateProccessTest {
  var uid = "wordcountUID"
  val ckpPath = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  val srcCkpPath = ckpPath + "/SocketScalaWordcountTest/202007030907/8eb64a706650df01b14e175b9ac75906/chk-20"
  val newPath = ckpPath + "/scalatanssavepoint"
  val bEnv = ExecutionEnvironment.getExecutionEnvironment
  def main(args: Array[String]): Unit = {
//    val existSp =
//      Savepoint.load(bEnv, newPath, new RocksDBStateBackend(ckpPath))
    // readKeyState(existSp, uid).print
    // transKeystateAndWritebak(existSp)

    // readTuple2KeyState(existSp, uid).print
    // transTuple2KeystateAndWritebak(existSp)


   // readPoJoKeyState(existSp, uid).print
   // transPojoKeystateAndWritebak(existSp)
  }

  /**
    * 读取 state数据
    * @param existSp
    * @param uid
    * @return
    */
  def readKeyState(existSp: ExistingSavepoint, uid: String) = {
    //
    existSp
      .readKeyedState(
        uid,
        new WordCounKeyreader("wordcountState")
      )
  }



  /**
   * 第一次读取ckp的时候需要加，否则报 错
   * createTypeInformation[(String, String)],
   * createTypeInformation[TransWordCount]
   * 当修改ckp之后，再读出来就要去掉。。。。但是写出来的chk不可用
   *
   * @param existSp
   * @param uid
   * @return
   */
  def readTuple2KeyState(existSp: ExistingSavepoint, uid: String) = {
    // 不加 createTypeInformation 默认用的是kryo序列话，原始数据是scalacassclass序列化
    // 但是再trans写入的时候又变成kryo序列化了，导致前后写入的序列化不一样
    import org.apache.flink.streaming.api.scala.createTypeInformation
    existSp
      .readKeyedState(
        uid,
        new WordCounTuple2Keyreader("wordcountState")
      )
  }


  /**
   * 第一次读取ckp的时候需要加，否则报 错
   * createTypeInformation[(String, String)],
   * createTypeInformation[TransWordCount]
   * 当修改ckp之后，再读出来就要去掉。。。。但是写出来的chk不可用
   *
   * @param existSp
   * @param uid
   * @return
   */
  def readPoJoKeyState(existSp: ExistingSavepoint, uid: String) = {
    // 不加 createTypeInformation 默认用的是kryo序列话，原始数据是scalacassclass序列化
    // 但是再trans写入的时候又变成kryo序列化了，导致前后写入的序列化不一样
    import org.apache.flink.streaming.api.scala.createTypeInformation
    existSp
      .readKeyedState(
        uid,
        new WordCounPojoKeyreader("wordcountState")
        //        ,
        //        createTypeInformation[(String, String)],
        //        createTypeInformation[TransWordCount]
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

  /**
   * 转换数据并写回savepoint
   * @param existSp
   */
  def transTuple2KeystateAndWritebak(existSp: ExistingSavepoint): Unit = {
    val oldState = readTuple2KeyState(existSp, uid)
    oldState.print()
    val transformation: BootstrapTransformation[TransWordCount] =
      OperatorTransformation
        .bootstrapWith(oldState)
        .keyBy(new KeySelector[TransWordCount, Tuple2[String, String]] {
          override def getKey(value: TransWordCount): Tuple2[String, String] =
            new Tuple2(value.word, value.word)
        })
        // .keyBy("word", "word") // 这个在KeyedStateBootstrapFunction 只能用K = Tuple，但是写回去就读不出来了
        .transform(new AccountTuple2KeyedStateBootstrapFunction)

    existSp
      .removeOperator(uid)
      .withOperator(uid, transformation)
      .write(newPath)

    bEnv.execute("FlinkKeyStateProccessTest") // print 会自动调用execute ，所以注释掉，否则报错
  }

  /**
   * 转换数据并写回savepoint
   * @param existSp
   */
  def transPojoKeystateAndWritebak(existSp: ExistingSavepoint): Unit = {
    val oldState = readPoJoKeyState(existSp, uid)
    oldState.print()
    val transformation: BootstrapTransformation[TransWordCount] =
      OperatorTransformation
        .bootstrapWith(oldState)
        .keyBy(new KeySelector[TransWordCount, WordCountGroupByKey] {
          override def getKey(value: TransWordCount): WordCountGroupByKey = {
            val k = new WordCountGroupByKey();
            k.setKey(value.word)
            k
          }
        })
        // .keyBy("word", "word") // 这个在KeyedStateBootstrapFunction 只能用K = Tuple，但是写回去就读不出来了
        .transform(new AccountPojoKeyedStateBootstrapFunction)

    existSp
      .removeOperator(uid)
      .withOperator(uid, transformation)
      .write(newPath)

    bEnv.execute("FlinkKeyStateProccessTest") // print 会自动调用execute ，所以注释掉，否则报错
  }
}
