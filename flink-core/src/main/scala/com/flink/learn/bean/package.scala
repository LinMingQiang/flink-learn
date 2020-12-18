package com.flink.learn

import scala.beans.BeanProperty

package object bean {

  case class AdlogBean(var plan: String,
                       var startdate: String,
                       var hour: String,
                       var pv: StatisticalIndic) {
    @BeanProperty
    val key = s"${startdate},${plan},${hour}"
    override def toString() = {
      (key, pv).toString()
    }
  }

  case class StatisticalIndic(var pv: Int) {
    override def toString() = {
      pv.toString
    }
  }

  /**
    *
    * @param key
    * @param count 统计值
    * @param lastModified 当前key最后更新时间。如果最后更新时间和当前时间或者event time 相差60s就认为是下一个session
    */
  case class CountWithTimestamp(key: String, count: Long, lastModified: Long)
}
