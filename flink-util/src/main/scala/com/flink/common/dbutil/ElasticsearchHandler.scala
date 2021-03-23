package com.flink.common.dbutil

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.data.RowData
import org.apache.flink.table.types.logical.{BigIntType, DoubleType, IntType, RowType, VarCharType}
import org.apache.flink.types.Row

import java.net.InetAddress
import org.elasticsearch.action.bulk.{BackoffPolicy, BulkProcessor, BulkRequest, BulkResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient
import org.slf4j.LoggerFactory

import scala.collection.mutable

object ElasticsearchHandler {
  lazy val _log = LoggerFactory.getLogger(ElasticsearchHandler.getClass)
  var client: TransportClient = null
  var preClient: PreBuiltXPackTransportClient = null;
  // 解决es 报错
  System.setProperty("es.set.netty.runtime.available.processors", "false")

  /**
    *
    * @param address
    * @return
    */
  def getGlobalEsClient(address: String,
                        clustername: String): TransportClient = {
    if (client == null) {
      println("init es client")
      val setting = Settings.builder().put("cluster.name", clustername).build()
      client = new PreBuiltTransportClient(setting)
      address.split(",").map(_.split(":", -1)).foreach {
        case Array(host, port) =>
          client.addTransportAddress(
            new TransportAddress(InetAddress.getByName(host), port.toInt))
        case Array(host) =>
          client.addTransportAddress(
            new TransportAddress(InetAddress.getByName(host), 9300))
      }
      println("init es success")
    }
    client
  }

  System.setProperty("es.set.netty.runtime.available.processors", "false")

  /**
    *
    * @param address
    * @return
    */
  def getGlobalEsPreClient(address: String,
                           clustername: String,
                           userPassw: String,
                           path: String = "elastic-certificates.p12")
    : PreBuiltXPackTransportClient = {
    if (preClient == null) {
      preClient = getEsPreClient(address, clustername, userPassw, path)
    }
    preClient
  }

  /**
    *
    * @param address
    * @param clustername
    * @param userPassw
    * @return
    */
  def getEsPreClient(address: String,
                     clustername: String,
                     userPassw: String,
                     path: String = "elastic-certificates.p12")
    : PreBuiltXPackTransportClient = {
    _log.info("init es client")
    //    val path = ElasticsearchHandler.getClass
    //      .getClassLoader()
    //      .getResource("elastic-certificates.p12")
    //      .getFile()
    val client = if (!userPassw.isEmpty) {
      new PreBuiltXPackTransportClient(
        Settings
          .builder()
          .put("cluster.name", clustername)
          .put("xpack.security.user", userPassw)
          .put("xpack.security.transport.ssl.enabled", true)
          .put("xpack.security.transport.ssl.verification_mode", "certificate")
          .put("xpack.security.transport.ssl.keystore.path", path)
          .put("xpack.security.transport.ssl.truststore.path", path)
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
    _log.info("init es success")
    client
  }

  /**
    *
    * @param address
    * @param clustername
    * @return
    */
  def getEsClient(address: String, clustername: String): TransportClient = {
    _log.info(" init es client ing ")
    val setting = Settings
      .builder()
      .put("cluster.name", clustername)
      .build()
    val client = new PreBuiltTransportClient(setting)
    address.split(",").map(_.split(":", -1)).foreach {
      case Array(host, port) =>
        client.addTransportAddress(
          new TransportAddress(InetAddress.getByName(host), port.toInt))
      case Array(host) =>
        client.addTransportAddress(
          new TransportAddress(InetAddress.getByName(host), 9300))
    }
    _log.info(" init es client success ")
    client
  }

  def upsertIndex(client: TransportClient, index: UpdateRequest)(
      adress: String,
      clustname: String): Unit = {
    if (client == null) getEsClient(adress, clustname).update(index).get()
    else client.update(index).get()
  }

  /**
    * 带自动提交功能
    */
  def bulkProcessBuild(actionNum: Int, interval: Long, retryT: Int)(
      client: TransportClient): BulkProcessor = {
    val bulkP = BulkProcessor
      .builder(
        client,
        new BulkProcessor.Listener() {
          //这个方法是在bulk执行前触发的。你可以在方法内request.numberOfActions()
          override def beforeBulk(executionId: Long,
                                  request: BulkRequest): Unit = {}

          //这个方法在bulk执行成功后触发的。你可以在方法内使用response.hasFailures()
          override def afterBulk(l: Long,
                                 bulkRequest: BulkRequest,
                                 bulkResponse: BulkResponse): Unit = {
            println("提交完成： ", bulkRequest.numberOfActions())
          }

          override def afterBulk(l: Long,
                                 bulkRequest: BulkRequest,
                                 throwable: Throwable): Unit = {
            println("提交失败： ", bulkRequest.numberOfActions(), throwable.toString)
            // 失败处理
          }

        }
      )
      .setBulkActions(actionNum) // 多少提交
      .setBackoffPolicy( // 失败重试
        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100),
                                         retryT))
      .setFlushInterval(TimeValue.timeValueSeconds(interval)) //几秒提交
      .build()
    bulkP
  }

  /**
    *
    * @param client
    * @param index
    * @param indextype
    */
  def creatadplatformIdIndex(client: TransportClient,
                             index: String,
                             indextype: String): Unit = {
    val creatIndexReq =
      client.admin().indices().prepareCreate(index)
    creatIndexReq.setSettings(
      Settings
        .builder()
        .put("index.number_of_shards", 5))
    val mapping = XContentFactory
      .jsonBuilder()
      .startObject()
      .startObject("properties")
      .startObject("day")
      .field("type", "keyword") // 设置数据类型
      .endObject()
    // .....
    mapping
      .endObject()
      .endObject()
    creatIndexReq.addMapping(indextype, mapping)
    creatIndexReq.execute().actionGet()
  }

  /**
    * 第一个字段是indexname。第二个是 key。后面是字段了。
    *
    * @param value
    * @return
    * @throws IOException
    */
  def createUpdateReqestFromRowData(
      indexName: String,
      idIndex: Int,
      value: RowData,
      fieldTypes: mutable.Buffer[RowType.RowField]): UpdateRequest = {
    val creatDoc = XContentFactory.jsonBuilder.startObject
    val id = value.getString(idIndex).toString
    for (i <- 0 until (fieldTypes.length-1)) {
      if (i != idIndex) {
        fieldTypes(i).getType match {
          case _ : VarCharType => creatDoc.field(fieldTypes(i).getName, value.getString(i).toString)
          case _ : DoubleType => creatDoc.field(fieldTypes(i).getName, value.getDouble(i))
          case _ : BigIntType => creatDoc.field(fieldTypes(i).getName, value.getLong(i))
          case _ : IntType => creatDoc.field(fieldTypes(i).getName, value.getInt(i))
          case _ => println("类型不对 。。。")
        }
      }
    }
    creatDoc.endObject
    val updater = new UpdateRequest(indexName, id)
      .upsert(creatDoc)
      .retryOnConflict(3)
      .doc(creatDoc)
    updater
  }

  def main(args: Array[String]): Unit = {}
}
