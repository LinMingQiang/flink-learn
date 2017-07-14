package com.flink.comom.dbUtil

import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import java.util.HashMap
import org.apache.hadoop.hbase.client.Table
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Get
object FlinkHbaseFactory {
  var conn: Connection = null
  var tables: HashMap[String, Table] = new HashMap[String, Table]
  def initConn() {
    if (conn == null || conn.isClosed()) {
      var hconf = HBaseConfiguration.create()
      hconf.set("hbase.zookeeper.quorum", HBASE_ZOOKEEPER)
      hconf.set("hbase.zookeeper.property.clientPort", "2181")
      conn = ConnectionFactory.createConnection(hconf)
    }
  }
  def getConn() = {
    initConn
    conn
  }
  def getTable(tablename: String) = {
    tables.getOrElse(tablename, {
      initConn
      conn.getTable(TableName.valueOf(tablename))
    })
  }
  def put(tableName: String, p: Put) {
    getTable(tableName)
      .put(p)
  }
  def get(tableName: String, get: Get, columns: Array[String]) = {
    val r = getTable(tableName)
            .get(get)
    if (r!=null && !r.isEmpty()) {
      columns.map { x => new String(r.getValue("info".getBytes, x.getBytes)) }
    }else null
  }
}