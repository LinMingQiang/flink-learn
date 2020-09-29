package com.flink.common.core

object CaseClassManager {
  case class WC(word: String, frequency: Int)
  case class Order(user: Long, product: String, amount: Int)

}
