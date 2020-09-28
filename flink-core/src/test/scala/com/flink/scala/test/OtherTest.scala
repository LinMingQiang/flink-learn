package com.flink.scala.test

import org.apache.commons.codec.binary.Hex

object OtherTest {

  /**
   * MD5加密
   *
   * @param s 输入字符串
   * @return MD5字符串
   */
  def encryptMd5_32(s: String): String = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    val r = new java.math.BigInteger(1, m.digest()).toString(16)
    val sb = new StringBuffer()

    if (r.length == 32) {
      r
    }
    else {
      for (_ <- 0 until 32 - r.length) {
        sb.append("0")
      }
      sb.append(r)
      sb.toString
    }

  }

  def main(args: Array[String]): Unit = {
    println(Hex.decodeHex(encryptMd5_32("ec:88:8f:81:a9:b2").toCharArray))
  }
}
