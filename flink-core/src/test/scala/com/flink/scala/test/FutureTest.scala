package com.flink.scala.test

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object FutureTest {
  def main(args: Array[String]): Unit = {
    val f: Future[String] = Future {
      Thread.sleep(4000L)
      "hell "
    }


    f.onComplete {
      case Success(s) =>
        println("Success ", s)
      case Failure(exception) =>
        println("Failure " , exception)
    }

    println(">>> 主线程无堵塞 <<<<")
    Thread.sleep(10000L)

  }
}
