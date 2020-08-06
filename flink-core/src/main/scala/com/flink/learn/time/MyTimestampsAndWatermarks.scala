package com.flink.learn.time

import com.flink.learn.bean.CaseClassUtil.Wordcount
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

class MyTimestampsAndWatermarks(maxOutOfOrderness: Long)
    extends AssignerWithPeriodicWatermarks[Wordcount] {
  var currentMaxTimestamp = 0L

  /**
    * 当前最大的时间戳 - 最多延迟多久
    * @return
    */
  override def getCurrentWatermark: Watermark = {
    val w = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    if(currentMaxTimestamp > 0)
      println(s"event t : ${currentMaxTimestamp} water : ${w.getTimestamp}")
    w
  }

  override def extractTimestamp(element: Wordcount, l: Long): Long = {
    currentMaxTimestamp = Math.max(element.timestamp, currentMaxTimestamp)
    element.timestamp
  }
}
