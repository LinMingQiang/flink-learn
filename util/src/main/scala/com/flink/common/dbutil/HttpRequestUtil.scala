package com.flink.common.dbutil

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object HttpRequestUtil {
  val _log = LoggerFactory.getLogger(HttpRequestUtil.getClass)
  def getHttpDefualtClient(connTime: Int,
                           socketTime: Int): DefaultHttpClient = {
    val httpclient = new DefaultHttpClient()
    httpclient
      .getParams()
      .setIntParameter("http.connection.timeout", connTime)
      .setIntParameter("http.socket.timeout", socketTime)
      .setIntParameter("http.socket.buffer-size", 0)
    httpclient
  }

  /**
    *
    * @param url
    * @param retry
    * @return
    */
  def getSlotMapping(url: String,
                     retry: Int = 3): Option[Map[String, String]] = {
    val httpclient = getHttpDefualtClient(100000, 100000)
    val post = new HttpPost(url)
    val resp = httpclient.execute(post)
    val str = EntityUtils.toString(resp.getEntity, "utf-8")
    try {
      if (retry > 0) {
        Some(
          JSON
            .parseObject(str)
            .getJSONArray("data")
            .asScala
            .map(d => {
              val j = JSON.parseObject(d.toString)
              s"${j.getString("adx_id")},${j.getString("slot_id")},${j
                .getString("appkey")},${j.getInteger("os")}" -> j
                .getString("mob_slot_id")
            })
            .toMap)
      } else None
    } catch {
      case t: Throwable =>
        _log.error(s"${str} : retry " + retry)
        Thread.sleep(1000)
        getSlotMapping(url, retry - 1)
    }
  }

  def main(args: Array[String]): Unit = {
  }
}
