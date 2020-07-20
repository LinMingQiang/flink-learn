package com.flink.common.dbutil
import org.apache.hadoop.hbase.client.{
  Connection,
  ConnectionFactory,
  Put,
  Table
}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.slf4j.LoggerFactory
import scala.collection.mutable
object HbaseHandler {
  var hbaseClient: Connection = null
  lazy val _log = LoggerFactory.getLogger(HbaseHandler.getClass)
  val tables = new mutable.HashMap[String, Table]()

  /**
    * 获取全局的conn
    * @param zk
    * @return
    */
  def getHbaseConn(zk: String): Connection = {
    if (hbaseClient == null || hbaseClient.isClosed) {
      _log.info(" init hbase connect ")
      val conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum", zk);
      conf.set("hbase.zookeeper.property.clientPort", "2181");
      hbaseClient = ConnectionFactory.createConnection(conf)
      _log.info(" init hbase success ")
    }
    hbaseClient
  }

  /**
    *
    * @param zk
    * @param tableName
    * @return
    */
  def getTable(zk: String, tableName: String): Table = {
    if (tables.contains(tableName)) {
      tables(tableName)
    } else {
      val tb = getHbaseConn(zk)
        .getTable(TableName.valueOf(tableName))
      tables.put(tableName, tb)
      tb
    }

  }

  /**
    * 组装put
    * @param rowkey
    * @param family
    * @param columns
    * @param v
    */
  def getPut(rowkey: String,
             family: Array[Byte],
             columns: Array[Byte],
             v: String): Put = {
    val put = new Put(rowkey.getBytes())
    put.addColumn(family, columns, v.getBytes)
    put
  }

  /**
    * updateoffset
    * @param tbl
    * @param rowkey
    * @param family
    * @param column
    * @param v
    */
  def setConsumterOffset(tbl: Table,
                         rowkey: String,
                         family: String,
                         column: String,
                         v: String): Unit = {
    val put = new Put(rowkey.getBytes())
    put.addColumn(family.getBytes(), column.getBytes(), v.getBytes())
    tbl.put(put)
  }

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

  }

}
