package com.flink.learn.richf

import com.flink.learn.bean.{AdlogBean, StatisticalIndic}
import com.flink.common.core.FlinkEvnBuilder
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class AdlogPVRichFlatMapFunction
    extends RichFlatMapFunction[AdlogBean, AdlogBean] {
  var lastState: ValueState[StatisticalIndic] = _
  /**
    * @author LMQ
    * @desc 每次一套
    * @param value
    * @param out
    */
  override def flatMap(value: AdlogBean, out: Collector[AdlogBean]): Unit = {
    val ls = lastState.value()
    if(ls == null) StatisticalIndic(0)
    val news = StatisticalIndic(ls.pv + value.pv.pv)
    lastState.update(news)
    value.pv = news
    out.collect(value)
  }

  /**
    * @author LMQ
    * @desc 当首次打开此operator的时候调用，拿到 此key的句柄
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    import org.apache.flink.streaming.api.scala._
    val desc = new ValueStateDescriptor[(StatisticalIndic)](
      "StatisticalIndic",
      createTypeInformation[(StatisticalIndic)])
    desc.enableTimeToLive(FlinkEvnBuilder.getStateTTLConf()) // TTL
    //desc.setQueryable("StatisticalIndic")
    lastState = getRuntimeContext().getState(desc)
  }

}
