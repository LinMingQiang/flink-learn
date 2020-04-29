package com.flink.learn.sql.func

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.AggregateFunction
import java.lang.{Long => JLong, Integer => JInteger}
import org.apache.flink.streaming.api.scala.createTypeInformation

/**
  * 加权平均值计算
  */
class WeightedAvgAggregateFunction
    extends AggregateFunction[JLong, WeightedAvgAccum] {

  /**
    * 缩回
    * @param acc
    * @param iValue
    * @param iWeight
    */
  def retract(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum -= iValue * iWeight
    acc.count -= iWeight
  }

  /**
    * 累加
    * @param acc
    * @param iValue
    * @param iWeight
    */
  def accumulate(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum += iValue * iWeight
    acc.count += iWeight
  }

  def resetAccumulator(acc: WeightedAvgAccum): Unit = {
    acc.count = 0
    acc.sum = 0L
  }

  override def getAccumulatorType: TypeInformation[WeightedAvgAccum] = {
    createTypeInformation[WeightedAvgAccum]
    // new TupleTypeInfo(classOf[WeightedAvgAccum], Types.LONG, Types.INT)
  }

  override def getResultType: TypeInformation[JLong] = Types.LONG
  /**
    *
    * @param acc
    * @param it
    */
  def merge(acc: WeightedAvgAccum,
            it: java.lang.Iterable[WeightedAvgAccum]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.count += a.count
      acc.sum += a.sum
    }
  }
  /*
   * 系统在每次aggregate计算完成后调用这个方法。
   */
  override def getValue(acc: WeightedAvgAccum): JLong = {
    if (acc.count == 0) {
      null
    } else {
      acc.sum / acc.count
    }
  }

  /**
    * 具有初始值的累加器
    * 初始化AggregateFunction的accumulator。
    * 系统在第一个做aggregate计算之前调用一次这个方法。
    */
  override def createAccumulator(): WeightedAvgAccum =
    WeightedAvgAccum(0L, 0)
}
/**
  * Accumulator for WeightedAvg.
  */
case class WeightedAvgAccum(var sum: JLong, var count: JInteger)
