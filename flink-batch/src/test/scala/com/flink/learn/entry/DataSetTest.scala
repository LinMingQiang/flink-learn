package com.flink.learn.entry

import com.flink.common.core.FlinkEvnBuilder
import org.apache.flink.api.scala._
object DataSetTest {
  def main(args: Array[String]): Unit = {
    val env = FlinkEvnBuilder.buildEnv() // 1 min
    val text = env.fromElements("Who's there?",
                                "I think I hear them. Stand, ho! Who's there?")

    val counts = text
      .flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print()
  }
}
