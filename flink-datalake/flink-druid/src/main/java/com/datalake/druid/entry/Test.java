package com.datalake.druid.entry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class Test {
    public static void main(String[] args) throws Exception {
        // 1. 加载Druid JDBC驱动
        Class.forName("org.apache.calcite.avatica.remote.Driver");
        // 2. 获取Druid JDBC连接
        Connection connection = DriverManager.getConnection(
                "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/", new Properties());
        // 3. 构建SQL语句
        String sql = "SELECT count(1) as aa FROM \"wikiticker-2015-09-12-sampled\"";
        // 4. 构建Statement，执行SQL获取结果集
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        // 5. 迭代ResultSet
        while(resultSet.next()) {
            long view_count = resultSet.getLong("aa");
            System.out.println( view_count);
        }
        // 6. 关闭Druid连接
        resultSet.close();
        statement.close();
        connection.close();
    }
}
