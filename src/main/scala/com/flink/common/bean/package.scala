package com.flink.common

import scala.beans.BeanProperty

package object bean {

  case class AdlogBean(
                        var plan: String,
                        var startdate: String,
                        var hour: String,
                        var pv: StatisticalIndic) {
    @BeanProperty
    val key = s"${startdate},${plan},${hour}"

    override def toString() = {
      (key, pv).toString()
    }
  }



  case class StatisticalIndic(var pv: Int){
    override def toString()={
      pv.toString
    }
  }

}
