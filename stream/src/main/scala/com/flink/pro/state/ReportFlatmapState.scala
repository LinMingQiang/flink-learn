package com.flink.pro.state

import com.flink.pro.common.CaseClassUtil.{ReportInfo, ReportValues}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class ReportFlatmapState(ttl: Long) extends RichFlatMapFunction[ReportInfo, ReportInfo] {
  var lastState: ValueState[ReportValues] = _

  /**
    * @author LMQ
    * @desc 当首次打开此operator的时候调用，拿到 此key的句柄
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    import org.apache.flink.streaming.api.scala._
    val desc = new ValueStateDescriptor[ReportValues](
      "StatisticalIndic",
      createTypeInformation[ReportValues])
    desc.enableTimeToLive(getStateTTLConf(ttl)) // TTL
    //desc.setQueryable("StatisticalIndic")
    lastState = getRuntimeContext.getState(desc)
  }

  /**
    * 累加结果并返回
    * @param in
    * @param collector
    */
  override def flatMap(in: ReportInfo,
                       collector: Collector[ReportInfo]): Unit = {
    try {
      var lastV = lastState.value()
      if (lastV == null) lastV = in.rv
      else {
        lastV.bidReqNum += in.rv.bidReqNum
        lastV.fillNum += in.rv.fillNum
        lastV.impressionNum += in.rv.impressionNum
        lastV.clickNum += in.rv.clickNum
        lastV.income += in.rv.income
      }
      lastState.update(lastV)
      collector.collect(in.copy(rv = lastV))
    } catch {
      case e: Throwable => println(e)
    }

  }

}
