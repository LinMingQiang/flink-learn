package com.sql.gateway.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class FLinkSqlGatewayJDBCTest {
    /**
     * jdbc 只支持 batch模式
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:flink://localhost:8083?planner=blink");
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE T(\n" +
                "  a INT,\n" +
                "  b VARCHAR(10)\n" +
                ") WITH (\n" +
                "  'connector.type' = 'filesystem',\n" +
                "  'connector.path' = 'file:///Users/eminem/workspace/flink/flink-learn/resources/file/sqlgateway/test2.csv',\n" +
                "  'format.type' = 'csv',\n" +
                "  'format.derive-schema' = 'true'\n" +
                ")");
        // 如果文件不存在就写入，如果存在insert不生效
//        statement.executeUpdate("INSERT INTO T VALUES (1, 'Hi'), (2, 'Hello')");
        ResultSet rs = statement.executeQuery("SELECT * FROM T");
        while (rs.next()) {
            System.out.println(rs.getInt(1) + ", " + rs.getString(2));
        }

        statement.close();
        connection.close();
    }
}
