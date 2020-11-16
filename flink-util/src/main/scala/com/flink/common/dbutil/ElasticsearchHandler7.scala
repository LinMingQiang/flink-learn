package com.flink.common.dbutil

import java.net.InetAddress

import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient
import org.slf4j.LoggerFactory

object ElasticsearchHandler7 {
  lazy val _log = LoggerFactory.getLogger(ElasticsearchHandler.getClass)
  var client: PreBuiltXPackTransportClient = null
  // 解决es 报错
  System.setProperty("es.set.netty.runtime.available.processors", "false")
  /**
    *
    * @param address
    * @return
    */
  def getGlobalEsClient(address: String,
                        clustername: String,
                        userPassw: String,
                        xpackPath: String): PreBuiltXPackTransportClient = {
    if (client == null) {
      client = getEsClient(address, clustername, userPassw, xpackPath)
    }
    client
  }

  /**
    *
    * @param address
    * @param clustername
    * @param userPassw
    * @return
    */
  def getEsClient(address: String,
                  clustername: String,
                  userPassw: String,
                  xpackPath: String): PreBuiltXPackTransportClient = {
    _log.info("------- init es client ------")
    val client = if (!userPassw.isEmpty) {
      new PreBuiltXPackTransportClient(
        Settings
          .builder()
          .put("cluster.name", clustername)
          .put("xpack.security.user", userPassw)
          .put("xpack.security.transport.ssl.enabled", true)
          .put("xpack.security.transport.ssl.verification_mode", "certificate")
          .put("xpack.security.transport.ssl.keystore.path", xpackPath)
          .put("xpack.security.transport.ssl.truststore.path", xpackPath)
          .put("thread_pool.search.size", 8)
          .build())
    } else {
      new PreBuiltXPackTransportClient(
        Settings
          .builder()
          .put("cluster.name", clustername)
          .build())
    }
    address.split(",").map(_.split(":", -1)).foreach {
      case Array(host, port) =>
        client.addTransportAddresses(
          new TransportAddress(InetAddress.getByName(host), port.toInt))
      case Array(host) =>
        client.addTransportAddresses(
          new TransportAddress(InetAddress.getByName(host), 9300))
    }
    _log.info("----- init es success -------")
    client
  }
}
