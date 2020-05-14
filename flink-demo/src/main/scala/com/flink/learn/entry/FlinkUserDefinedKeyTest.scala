package com.flink.learn.entry

import com.flink.common.core.{
  EnvironmentalKey,
  FlinkEvnBuilder,
  FlinkLearnPropertiesUtil
}
import com.flink.common.core.FlinkLearnPropertiesUtil.{
  FLINK_DEMO_CHECKPOINT_PATH,
  param
}
import org.apache.flink.api.scala._
object FlinkUserDefinedKeyTest {
  case class UserDefinedKey(name: String, age: Int)

  /**
    * 用户自定义key
    * @param args
    */
  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "KafkaWordCountTest")
    val env = FlinkEvnBuilder.buildStreamingEnv(param,
                                                FLINK_DEMO_CHECKPOINT_PATH,
                                                10000) // 1 min
    env
      .fromElements("a", "b", "a", "a", "a")
      .map(x => { (UserDefinedKey(x, x.hashCode), 1) })
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }
}
