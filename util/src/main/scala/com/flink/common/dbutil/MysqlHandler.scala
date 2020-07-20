package com.flink.common.dbutil

import java.sql.{Connection, DriverManager}

object MysqlHandler {
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

  def main(args: Array[String]): Unit = {
    val jdbc = "jdbc:mysql://10.18.97.129:3306/bgm_ssp_test"
    val user = "root"
    val passw = "123456"
    val conn = getMysqlConn(jdbc, user, passw)
    // val sql = s"""replace into $tb(id,aa,bb) values(?,?,?)"""
//    val sql =
//      s"""insert into $tb(id,aa,bb) values(?,?,?)
//         | ON DUPLICATE KEY UPDATE
//         |  id =VALUES(id), aa = VALUES(aa) , bb = VALUES(bb)""".stripMargin



    conn.close()
  }

}
