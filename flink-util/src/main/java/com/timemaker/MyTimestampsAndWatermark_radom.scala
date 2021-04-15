package com.timemaker

import com.flink.common.kafka.KafkaManager.KafkaMessge
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark

/**
  *
  * @param maxOutOfOrderness 最长延迟时间
  */
class MyTimestampsAndWatermark_radom(maxOutOfOrderness: Long)
    extends AssignerWithPeriodicWatermarks[KafkaMessge] {
  var currentMaxTimestamp = 0L // 当前数据最大的时间戳

  /**
    * @return 获取wartermark。用当前最大的数据时间戳- 最多延迟多久
    */
  override def getCurrentWatermark: Watermark = {

    val w = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
    println(w)
    w
  }

  /**
    * 提取数据里面的时间戳
    * @param element
    * @param l
    * @return
    */
  override def extractTimestamp(element: KafkaMessge, l: Long): Long = {
    println(element)
    // 更新当前的最大时间戳
    currentMaxTimestamp = Math.max(System.currentTimeMillis(), currentMaxTimestamp)
    System.currentTimeMillis()
  }
}
