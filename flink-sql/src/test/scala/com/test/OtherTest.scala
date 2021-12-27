package com.test

import com.flink.common.util.StringUtils

import scala.collection.mutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object OtherTest {
  def main(args: Array[String]): Unit = {
    val file = "/Users/eminem/Desktop/ss.txt"
    var re = 0.0;
    Source.fromFile(file).getLines().foreach(x => {
      if (x.startsWith("+")) {
        println(x)
        re += x.toDouble
      }
    })
    println(re)
  }
}