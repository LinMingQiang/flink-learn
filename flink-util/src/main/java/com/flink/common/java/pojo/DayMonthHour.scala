package com.flink.common.java.pojo

class DayMonthHour extends Serializable {
  var d: String = null;
  var m: String = null;
  var h: String = null;

  override def toString = s"DayMonthHour($d, $m, $h)"
}
