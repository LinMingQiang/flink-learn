package com.flink.learn.entry

import java.util.Date

import com.flink.common.core.{
  EnvironmentalKey,
  FlinkEvnBuilder,
  FlinkLearnPropertiesUtil
}
import com.flink.common.core.FlinkLearnPropertiesUtil._
import com.flink.learn.bean.CaseClassUtil.SessionLogInfo
import com.flink.learn.param.PropertiesUtil
import com.flink.learn.richf.SessionWindowRichF
import com.flink.learn.time.MyTimestampsAndWatermarks2
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SessionWindowTest {

  def main(args: Array[String]): Unit = {
    FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH,
                                  "LocalFlinkTest")
    val env = FlinkEvnBuilder.buildStreamingEnv(PropertiesUtil.param,
                                                FLINK_DEMO_CHECKPOINT_PATH,
                                                60000) // 1 min
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 时间设为eventime
    env.getConfig.setAutoWatermarkInterval(5000L) // 每5s更新一次wartermark
    env
      .socketTextStream("localhost", 9876)
      .map(x => SessionLogInfo(x, new Date().getTime))
      .assignTimestampsAndWatermarks(new MyTimestampsAndWatermarks2(0))
      .keyBy(x => x.sessionId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(6)))
      .apply(new SessionWindowRichF)
      .print()

    env.execute("test")
  }
}
