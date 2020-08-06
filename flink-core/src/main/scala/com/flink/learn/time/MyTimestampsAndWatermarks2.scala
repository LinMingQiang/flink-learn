package com.flink.learn.time

import com.flink.learn.bean.CaseClassUtil.{SessionLogInfo, Wordcount}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  *
  * @param maxOutOfOrderness 最长延迟时间
  */
class MyTimestampsAndWatermarks2(maxOutOfOrderness: Long)
    extends AssignerWithPeriodicWatermarks[SessionLogInfo] {
  var currentMaxTimestamp = 0L // 当前数据最大的时间戳

  /**
    * @return 获取wartermark。用当前最大的数据时间戳- 最多延迟多久
    */
  override def getCurrentWatermark: Watermark = {
    new Watermark(currentMaxTimestamp - maxOutOfOrderness)
  }

  /**
    * 提取数据里面的时间戳
    * @param element
    * @param l
    * @return
    */
  override def extractTimestamp(element: SessionLogInfo, l: Long): Long = {
    // 更新当前的最大时间戳
    currentMaxTimestamp = Math.max(element.timeStamp, currentMaxTimestamp)
    element.timeStamp
  }
}
