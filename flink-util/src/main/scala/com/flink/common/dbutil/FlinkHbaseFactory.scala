package com.flink.common.dbutil

import java.util
import java.util.{ArrayList, HashMap}

import com.flink.common.core.EnvironmentalKey
import org.apache.flink.api.java.tuple.Tuple2
//import com.stumbleupon.async.Callback
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
//import org.hbase.async.{GetRequest, HBaseClient, KeyValue}

import scala.collection.JavaConversions._

object FlinkHbaseFactory {
  var conn: Connection = null
//  var asyncClient : HBaseClient = null
  def initConn(zk: String) {
    if (conn == null || conn.isClosed()) {
      println("----  Init Conn  -----")
      val hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", zk)
      hconf.set("hbase.zookeeper.property.clientPort", "2181")
      conn = ConnectionFactory.createConnection(hconf)
    }
  }

  def getGlobalConn(zk: String) = {
    initConn(zk)
    conn
  }

  def getTable(zk: String, table: String): Table ={
    initConn(zk)
    conn.getTable(TableName.valueOf(table))
  }

  def get(conn: Connection, tablename: String, key: String): Result = {
    val test = conn.getTable(TableName.valueOf(tablename))
    test.get(new Get(key.getBytes()))
      // .getValue("info".getBytes(), "v".getBytes())
  }
//  def getGlobalAsyncConn(zk: String) ={
//  if(asyncClient == null){
  //      asyncClient = new HBaseClient(zk)
  //    }
  //    asyncClient
//  }

  def main(args: Array[String]): Unit = {
//    getGlobalAsyncConn("localhost:2181")
//    val r = asyncClient.get(new GetRequest("test".getBytes, "test".getBytes()))
//    val rr = r.addCallback(new Callback[String, util.ArrayList[KeyValue]](){
//      override def call(t: util.ArrayList[KeyValue]): String = {
//        print(",,,,,")
//        t.map(x => {
//          println(x)
//          new String(x.value())
//        }).mkString("|")
//      }
//    })
//    asyncClient.shutdown()

    val conn = getGlobalConn("localhost:2181")
    conn.getAdmin.listTableNames().foreach(println)
    val test = conn.getTable(TableName.valueOf("test"))
    println(test.exists(new Get("test".getBytes())))
//    println(test.get(new Get("test".getBytes())).getValue("info".getBytes(),"v".getBytes()))
    conn.close()
    // println(">>>>>>>" + r)
  }

}
