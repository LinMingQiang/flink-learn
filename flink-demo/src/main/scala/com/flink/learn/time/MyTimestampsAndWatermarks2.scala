package com.flink.learn.time

import com.flink.learn.bean.CaseClassUtil.{SessionLogInfo, Wordcount}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class MyTimestampsAndWatermarks2(maxOutOfOrderness: Long)
    extends AssignerWithPeriodicWatermarks[SessionLogInfo] {
  var currentMaxTimestamp = 0L

  /**
    * 当前最大的时间戳 - 最多延迟多久
    * @return
    */
  override def getCurrentWatermark: Watermark = {
    val w = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    w
  }

  override def extractTimestamp(element: SessionLogInfo, l: Long): Long = {
    currentMaxTimestamp = Math.max(element.timeStamp, currentMaxTimestamp)
    element.timeStamp
  }
}
