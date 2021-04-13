package com.flink.scala.test

import org.apache.commons.codec.binary.Hex

import scala.io.Source

object Md5Test {

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
    } else {
      for (_ <- 0 until 32 - r.length) {
        sb.append("0")
      }
      sb.append(r)
      sb.toString
    }

  }

  def main(args: Array[String]): Unit = {

    val source = Source
      .fromFile("/Users/eminem/Desktop/x.txt")
      .getLines()
      .map(x => {
        (x, encryptMd5_32(x))
      })
    val idfa = Source
      .fromFile("/Users/eminem/Desktop/idfa.txt")
      .getLines()
      .map(x => x -> 0)
      .toMap
    val imeimd5 = Source
      .fromFile("/Users/eminem/Desktop/imeimd5.txt")
      .getLines()
      .map(x => x -> 0)
      .toMap
    val oiid = Source
      .fromFile("/Users/eminem/Desktop/oaid.txt")
      .getLines()
      .map(x => x -> 0)
      .toMap
    var c = 0;
    source.foreach {
      case (src, md5) =>
        if (idfa.contains(src.toUpperCase())) {
          println("IDFA : ", src)
          c += 1
        } else if (oiid.contains(src.toLowerCase())) {
          println("OAID : ", src)
          c += 1
        } else if (imeimd5.contains(md5)) {
          println("MD5 : ", md5)
          c += 1
        }
    }
    println(c)
//    println(Hex.decodeHex(encryptMd5_32("ec:88:8f:81:a9:b2").toCharArray))
  }
}
