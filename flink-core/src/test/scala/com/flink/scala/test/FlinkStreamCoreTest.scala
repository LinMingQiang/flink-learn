package com.flink.scala.test

import java.util.Date

import com.alibaba.fastjson.JSON
import com.flink.common.core.FlinkEvnBuilder
import com.flink.common.core.FlinkLearnPropertiesUtil._
import com.flink.common.deserialize.TopicMessageDeserialize
import com.flink.common.java.timemaker.MyTimestampsAndWatermark_radom
import com.flink.common.kafka.KafkaManager
import com.flink.common.kafka.KafkaManager.{KafkaMessge, KafkaTopicOffsetMsg}
import com.flink.learn.bean.CaseClassUtil.SessionLogInfo
import com.flink.learn.bean.{AdlogBean, CaseClassUtil, StatisticalIndic}
import com.flink.learn.richf.{AdlogPVRichFlatMapFunction, SessiontProcessFunction, SessionWindowRichF, WordCountRichFunction}
import com.flink.learn.sink.{OperatorStateBufferingSink, StateRecoverySinkCheckpointFunc}
import com.flink.learn.test.common.FlinkStreamCommonSuit
import com.flink.learn.time.MyTimestampsAndWatermarks2
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows, TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{EventTimeTrigger, ProcessingTimeTrigger}

class FlinkStreamCoreTest extends FlinkStreamCommonSuit {

  case class UserDefinedKey(name: String, age: Int)

  test("wordCount") {
    env
      .addSource(kafkaSource(TEST_TOPIC, BROKER))
      .flatMap(_.msg.split("\\|", -1))
      .map(x => (x, 1))
      .keyBy(0)
      .flatMap(new WordCountRichFunction)
      .print
    env.execute("lmq-flink-demo") //程序名
  }

  /**
   * 测试不进行checkpoint的情况下的state，针对大state，选择不chk
   */
  test("stateWithnoChk") {
    env.addSource(kafkaSource(TEST_TOPIC, BROKER))
      .flatMap(_.msg.split("\\|", -1))
      .map(x => (x, 1))
      .keyBy(0)
      .flatMap(new WordCountRichFunction)
      .print
    env.execute("lmq-flink-demo") //程序名
  }

  /**
   * session; 注意： 只有下一条数据来了，才会打印上一个窗口的结果。
   * 因为下一条数据的时间会更新wartermark，
   * 当wartermark更新后，才能确定某个窗口的数据永远不再更新了才会打印出来，否则一直等。
   * window 有两种 Windowfunction ：
   * AggregateFunction(一条一条聚合)
   * ProcessindowFunction 触发的时候一次性聚合， Iterator给你数据
   */
  test("sessionWindowWatermark") {
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 时间设为eventime
    env.getConfig.setAutoWatermarkInterval(5000L) // 每5s更新一次wartermark

    env
      .addSource(kafkaSource(TEST_TOPIC, BROKER))
      .map(x => {
        SessionLogInfo(x.msg, new Date().getTime)
      })
      .assignTimestampsAndWatermarks( // new MyTimestampsAndWatermarks2(10)
        new BoundedOutOfOrdernessTimestampExtractor[SessionLogInfo](Time.seconds(10)) {
        override def extractTimestamp(element: SessionLogInfo): Long = element.timeStamp
      }) // 0的话表示不延迟
      .keyBy(x => x.sessionId)
      .window(EventTimeSessionWindows.withGap(Time.seconds(6)))
      .trigger(EventTimeTrigger.create()) // 以eventtime 时间触发窗口，当wartermark 》 window endtime 触发
      .apply(new SessionWindowRichF)
      .print()
    env.execute("sessionWindowWatermark")
  }

  /**
   * 翻转窗口
   */
  test("tumblingWindows") {
    env
      .addSource(kafkaSource("test", "localhost:9092"))
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[KafkaTopicOffsetMsg](Time.seconds(2)) {
        override def extractTimestamp(element: KafkaTopicOffsetMsg): Long =
          {
            System.currentTimeMillis()}
      })
      .map(x => (x.msg, 1))
      .setParallelism(1)
      .keyBy(0)
      // .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) // 算session
     .window(TumblingEventTimeWindows.of(Time.seconds(4))) // = .timeWindow(Time.seconds(60))
     // .evictor(TimeEvictor.of(Time.seconds(2))) // 只保留 窗口内最近2s的数据做计算
     // .trigger(ProcessingTimeTrigger.create())
      .trigger(EventTimeTrigger.create()) // 以eventtime 时间触发窗口，当wartermark 》 window endtime 触发
      .sum(1) // 10s窗口的数据
      .print
    // .setParallelism(2)

    env.execute("jobname")
  }

  /**
   * operate状态恢复
   *
   * @param env
   */
  def StateRecoverySink(env: StreamExecutionEnvironment): Unit = {
    val result = env
      .addSource(kafkaSource(TEST_TOPIC, BROKER))
      .map { x =>
        val datas = x.msg.split(",")
        val statdate = datas(0).substring(0, 10) //日期
        val hour = datas(0).substring(11, 13) //hour
        val name = datas(1)
        if (name.nonEmpty) {
          AdlogBean(name, statdate, hour, StatisticalIndic(1))
        } else null
      }
      .filter { x =>
        x != null
      }
      .keyBy(_.key) //按key分组，可以把key相同的发往同一个slot处理
      .flatMap(new AdlogPVRichFlatMapFunction) //通常都是用的flatmap，功能类似 (filter + map)
    //operate state。用于写hbase是吧恢复
    result.addSink(new StateRecoverySinkCheckpointFunc(50))
    // result.addSink(new SystemPrintSink[AdlogBean]())
    //result.addSink(new HbaseReportSink)
    env.execute()
  }

  /**
   * operate
   *
   * @param env
   */
  def operateState(env: StreamExecutionEnvironment): Unit = {
    env
      .addSource(kafkaSource(TEST_TOPIC, BROKER))
      .map(x => (JSON.parseObject(x.msg).getString("dist"), 1))
      .keyBy(0)
      .sum(1)
      .addSink(new OperatorStateBufferingSink(1))

    env.execute("FlinkOperatorStateTest")
  }

  /**
   * process func
   *
   * @param env
   */
  def processFunc(env: StreamExecutionEnvironment): Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    //    val source = KafkaManager.getKafkaSource(
    //      TOPIC,
    //      BROKER,
    //      new TopicOffsetMsgDeserialize())
    //    kafkasource.setCommitOffsetsOnCheckpoints(true)
    //    kafkasource.setStartFromLatest() //不加这个默认是从上次消费
    // env
    //      .addSource(source)
    env
      .socketTextStream("localhost", 9876)
      .map(x => CaseClassUtil.SessionLogInfo(x, new Date().getTime))
      .keyBy(_.sessionId)
      .process(new SessiontProcessFunction)
      .print
    env.execute("FlinkOperatorStateTest")
  }

  def impressClick(env: StreamExecutionEnvironment): Unit = {
    // 同时支持多个流地运行
    getImpressDStream(env).addSink(new SinkFunction[(String, Int)] {
      override def invoke(value: (String, Int)): Unit = {
        println(value)
      }
    })
    getClickDStream(env).addSink(new SinkFunction[(String, Int)] {
      override def invoke(value: (String, Int)): Unit = {
        println(value)
      }
    })
    env.execute()
  }

  def userDefinedKey(env: StreamExecutionEnvironment): Unit = {
    env
      .fromElements("a", "b", "a", "a", "a")
      .map(x => {
        (UserDefinedKey(x, x.hashCode), 1)
      })
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }

  /**
   *
   * @param env
   * @return
   */
  def getImpressDStream(env: StreamExecutionEnvironment) = {

    val kafkasource2 = KafkaManager.getKafkaSource[KafkaMessge](
      "testimpress",
      BROKER,
      new TopicMessageDeserialize())
    kafkasource2.setCommitOffsetsOnCheckpoints(true)
    kafkasource2.setStartFromEarliest() //不加这个默认是从上次消费
    env
      .addSource(kafkasource2)
      .flatMap(_.msg.split("\\|", -1))
      .map(x => (x, 1))
      .keyBy(0)
      .flatMap(new WordCountRichFunction)
  }

  /**
   *
   * @param env
   * @return
   */
  def getClickDStream(env: StreamExecutionEnvironment) = {
    val kafkasource = KafkaManager.getKafkaSource[KafkaMessge](
      TEST_TOPIC,
      BROKER,
      new TopicMessageDeserialize())
    kafkasource.setCommitOffsetsOnCheckpoints(true)
    kafkasource.setStartFromEarliest() //不加这个默认是从上次消费
    env
      .addSource(kafkasource)
      .flatMap(_.msg.split("\\|", -1))
      .map(x => (x, 1))
      .keyBy(0)
      .flatMap(new WordCountRichFunction)
  }
}
