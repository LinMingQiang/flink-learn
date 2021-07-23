package com.flink.common.dbutil

import java.sql.{Connection, DriverManager}

object MysqlJdbcHandler {
  var conn: Connection = null
  /*
   *
   */
  def getMysqlConn(jdbc: String, user: String, pass: String): Connection = {
    // scalastyle:off
    Class.forName("com.mysql.jdbc.Driver")
    // scalastyle:on
    DriverManager.getConnection(jdbc, user, pass)
  }

  /**
   * 获取mysq的全局连接
   * @param jdbc
   * @param user
   * @param pass
   * @return
   */
  def getMysqlGlobalConn(jdbc: String,
                         user: String,
                         pass: String): Connection = {
    initMysql(jdbc, user, pass)
    conn
  }

  /**
   * 初始化mysql连接
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

  def deleteData(jdbc: String, user: String, pass: String, sql: String): Unit = {
    try {
      val con = getMysqlConn(jdbc, user, pass)
      val stmt = con .createStatement()
      stmt.executeUpdate(sql)
      stmt.close()
      con.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {
    val jdbc = "jdbc:mysql://localhost:3306/test"
    val user = "root"
    val passw = "123456"
    val conn = getMysqlConn(jdbc, user, passw)
    val sql = "select * from test"
    // val sql = s"""replace into $tb(id,aa,bb) values(?,?,?)"""
    //    val sql =
    //      s"""insert into $tb(id,aa,bb) values(?,?,?)
    //         | ON DUPLICATE KEY UPDATE
    //         |  id =VALUES(id), aa = VALUES(aa) , bb = VALUES(bb)""".stripMargin
    conn.close()

  }

}
