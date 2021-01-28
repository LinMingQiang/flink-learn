package com.calcite.test;

import com.calcite.sql.parser.impl.CalciteTestSqlParserImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.flink.sql.parser.dql.SqlShowCatalogs;

public class Test {
    public static void main(String[] args) {
        // https://blog.csdn.net/ccllcaochong1/article/details/93367343
        // SqlParserImpl calcite内嵌的；CalciteTestSqlParserImpl为自己定义的
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.configBuilder()
                        .setParserFactory(CalciteTestSqlParserImpl.FACTORY) // javacc编译出来的自定义的
                        .setCaseSensitive(false)
                        .setQuoting(Quoting.BACK_TICK)
                        .setQuotedCasing(Casing.TO_UPPER)
                        .setUnquotedCasing(Casing.TO_UPPER)
                        .setConformance(SqlConformanceEnum.ORACLE_12)
                        .build())
                .build();



        String sql = "SELECT u.name,sum(o.amount) AS total\n" +
                "         FROM orders o\n" +
                "         INNER JOIN users u ON o.uid = u.id\n" +
                "         WHERE u.age < 27\n" +
                "         GROUP BY u.name";
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        try {
            SqlNode sqlNode = parser.parseStmt();

            System.out.println(sqlNode.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
