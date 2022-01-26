package com.flink.common.dbutil

import java.sql.{Connection, DriverManager}

object ClickhouseFactory {

  var conn: Connection = null

  /*
   *
   */
  def getMysqlConn(jdbc: String, user: String, pass: String): Connection = {
    // scalastyle:off
    Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
    // scalastyle:on
    DriverManager.getConnection(jdbc, user, pass)
  }

  def getGlobalConn(jdbc: String,
                    user: String,
                    pass: String): Connection = {
    initMysql(jdbc, user, pass)
    conn
  }

  /**
   * 初始化mysql连接
   *
   * @param jdbc
   * @param user
   * @param pass
   */
  def initMysql(jdbc: String, user: String, pass: String) {
    if (null == conn || conn.isClosed) {
      // scalastyle:off
      Class.forName("com.mysql.jdbc.Driver")
      // scalastyle:on
      conn = DriverManager.getConnection(jdbc, user, pass)
    }
  }

  /**
   * clickhouse不支持 upsert的操作
   * 分片 = 节点
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val connection = getMysqlConn("jdbc:clickhouse://localhost:8123/", "default", "")
    // SELECT engine_full FROM system.tables WHERE database = 'default' AND name = 'test_local' // 只有分布式表才有集群信息
    // 插入
    val sql = "INSERT INTO test_local (id,name) VALUES (?,?)"
    val statement = connection.prepareStatement(sql)
    statement.setInt(1, 1);
    statement.setString(2, "test11");
    statement.addBatch()
    statement.setInt(1, 2);
    statement.setString(2, "test2111");
    statement.addBatch()
    statement.executeBatch()
    statement.close()
//    connection.close()
    // INSERT INTO test.test VALUES(1,'tom'),(2,'jack')

    // 查询
    val statement2 = connection.createStatement
    val results = statement2.executeQuery("select * from test_local")
    while (results.next()) {
      println(results.getString(2));
    }
  }
}
