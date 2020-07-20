package com.flink.pro.state

import com.flink.pro.common.CaseClassUtil.SourceLogData
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  * reqid 去重
  */
class ReportFlatmapDistincReqID(ttl: Long)
    extends RichFlatMapFunction[SourceLogData, SourceLogData] {
  var lastState: ValueState[String] = _

  /**
    * @author LMQ
    * @desc 当首次打开此operator的时候调用，拿到 此key的句柄
    * @param parameters
    */
  override def open(parameters: Configuration): Unit = {
    import org.apache.flink.streaming.api.scala._
    val desc = new ValueStateDescriptor[String]("StatisticalIndic",
                                                createTypeInformation[String])
    desc.enableTimeToLive(getStateTTLConf(ttl)) // TTL 默认2小时
    //desc.setQueryable("StatisticalIndic")
    lastState = getRuntimeContext().getState(desc)
  }

  /**
    *
    * @param value
    * @param out
    */
  override def flatMap(value: SourceLogData,
                       out: Collector[SourceLogData]): Unit = {
    try {
      val lastV = lastState.value()
      if (lastV == null) {
        out.collect(value)
      } else {
        // 取决于过滤规则，但是目前只能以第一个为基准，因为第一个的数据已经统计进去了，后面再来的只能扔
      }
      lastState.update(value.data(1))
    } catch {
      case e: Throwable => println(e)
    }

  }

}
