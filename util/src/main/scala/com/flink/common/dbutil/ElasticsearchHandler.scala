package com.flink.common.dbutil

import java.net.InetAddress

import org.apache.commons.codec.digest.DigestUtils
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.slf4j.LoggerFactory
import org.elasticsearch.action.bulk.{
  BackoffPolicy,
  BulkProcessor,
  BulkRequest,
  BulkResponse
}
import org.elasticsearch.common.unit.TimeValue
object ElasticsearchHandler {
  lazy val _log = LoggerFactory.getLogger(ElasticsearchHandler.getClass)
  var client: TransportClient = null
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
  def bulkProcessBuild(actionNum: Int, interval: Long, retryT : Int)(client: TransportClient): BulkProcessor = {
    val bulkP = BulkProcessor.builder(
      client,
      new BulkProcessor.Listener() {
        //这个方法是在bulk执行前触发的。你可以在方法内request.numberOfActions()
        override def beforeBulk(executionId: Long,
                                request: BulkRequest): Unit = {
        }
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
        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), retryT))
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
      .startObject("hour") // 字段id
      .field("type", "keyword") // 设置数据类型
      .endObject()
      .startObject("adplatformId")
      .field("type", "long")
      .endObject()
      .startObject("fillNum")
      .field("type", "long")
      .endObject()
      .startObject("bidReqNum")
      .field("type", "long")
      .endObject()
      .startObject("impressionNum")
      .field("type", "long")
      .endObject()
      .startObject("clickNum")
      .field("type", "long")
      .endObject()
      .startObject("income")
      .field("type", "long")
      .endObject()
    mapping
      .endObject()
      .endObject()
    creatIndexReq.addMapping(indextype, mapping)
    creatIndexReq.execute().actionGet()
  }

  /**
    *
    * @param client
    * @param index
    * @param indextype
    */
  def creatmediaIdIndex(client: TransportClient,
                        index: String,
                        indextype: String): Unit = {
    val creatIndexReq =
      client.admin().indices().prepareCreate(index)
    creatIndexReq.setSettings(
      Settings
        .builder()
        .put("index.number_of_shards", 20))
    val mapping = XContentFactory
      .jsonBuilder()
      .startObject()
      .startObject("properties")
      .startObject("day")
      .field("type", "keyword") // 设置数据类型
      .endObject()
      .startObject("hour") // 字段id
      .field("type", "keyword") // 设置数据类型
      .endObject()
      .startObject("adslotId")
      .field("type", "keyword")
      .endObject()
// 20191030新增字段
      .startObject("policyId")
      .field("type", "long")
      .endObject()
//      .startObject("countryCode")
//      .field("type", "keyword")
//      .endObject()
//      .startObject("provinceCode")
//      .field("type", "keyword")
//      .endObject()
//      .startObject("cityCode")
//      .field("type", "keyword")
//      .endObject()
      .startObject("adplatformId")
      .field("type", "long")
      .endObject()
// -- 20191030新增字段
      .startObject("appId")
      .field("type", "keyword")
      .endObject()
      .startObject("fillNum")
      .field("type", "long")
      .endObject()
      .startObject("bidReqNum")
      .field("type", "long")
      .endObject()
      .startObject("impressionNum")
      .field("type", "long")
      .endObject()
      .startObject("clickNum")
      .field("type", "long")
      .endObject()
      .startObject("income")
      .field("type", "long")
      .endObject()
    mapping
      .endObject()
      .endObject()
    creatIndexReq.addMapping(indextype, mapping)
    creatIndexReq.execute().actionGet()
  }
  def main(args: Array[String]): Unit = {

  }
}

// scalastyle:off
// 删除。
//POST index_name/index_type/_delete_by_query?wait_for_completion=false
//{
//  "query": {
//    "bool": {
//      "filter": {
//          "range": {
//            "day": {
//              "gte": "2019-11-05",
//              "lte": "2019-11-05"
//            }
//          }
//
//        }
//    }
//  }
//}
