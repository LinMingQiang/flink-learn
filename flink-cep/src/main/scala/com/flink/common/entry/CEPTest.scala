package com.flink.common.entry

import com.flink.common.core.FlinkEvnBuilder
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
object CEPTest {
  val cp = "file:///Users/eminem/workspace/flink/flink-learn/checkpoint"
  def main(args: Array[String]): Unit = {
    PropertiesUtil.init("proPath");

    val env = FlinkEvnBuilder.buildFlinkEnv(PropertiesUtil.param, cp, 60000)
    val loginEventStream = env.fromCollection(
      List(
        LoginEvent(1, "192.168.0.1", "fail", 1),
        LoginEvent(1, "192.168.0.2", "fail", 2),
        LoginEvent(1, "192.168.0.3", "fail", 3),
        LoginEvent(2, "192.168.10,10", "success", 4)
      ))

    // 5s 内连续失败两次
//    val loginFailPattern = Pattern
//      .begin[LoginEvent]("start")
//      .where(_.eventType.equals("fail"))
//      .times(2)
//      .within(Time.seconds(5))
    // 5s 内连续失败两次
    val loginFailPattern =
      Pattern
        // 定义第一个失败事件模式
        .begin[LoginEvent]("start").where(_.eventType == "fail")
        .next("next").where(_.eventType == "fail")
        .within(Time.seconds(5))

    CEP
      .pattern(loginEventStream.keyBy("userId"), loginFailPattern)
      .select(new LoginFailDetect)
      .print

    env.execute

  }
  class LoginFailDetect extends PatternSelectFunction[LoginEvent, Warning] {
    override def select(
        map: java.util.Map[String, java.util.List[LoginEvent]]): Warning = {
      val firstFailEvent = map("start").iterator.next()
      val secondFailEvent = map("next").iterator.next()
      Warning(firstFailEvent.userId,
              firstFailEvent.eventTime,
              secondFailEvent.eventTime,
              "login fail 2 times")
    }
  }

  // 输入的登录事件样例类
  case class LoginEvent(userId: Long,
                        ip: String,
                        eventType: String,
                        eventTime: Long)

  // 输出的报警信息样例类
  case class Warning(userId: Long,
                     firstFailTime: Long,
                     lastFailTime: Long,
                     waringMsg: String)
}
