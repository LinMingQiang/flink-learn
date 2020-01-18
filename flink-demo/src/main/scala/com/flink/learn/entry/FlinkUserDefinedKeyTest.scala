package com.flink.learn.entry

import com.flink.common.core.FlinkEvnBuilder
import com.flink.learn.param.PropertiesUtil
import org.apache.flink.api.scala._
object FlinkUserDefinedKeyTest {
  case class UserDefinedKey(name: String, age: Int)

  /**
    * 用户自定义key
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildStreamingEnv(PropertiesUtil.param, cp, 60000) // 1 min

    env
      .fromElements("a", "b", "a", "a", "a")
      .map(x => { (UserDefinedKey(x, x.hashCode) , 1)})
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }
}
