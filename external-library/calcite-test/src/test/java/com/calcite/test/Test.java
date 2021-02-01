package com.calcite.test;

import com.calcite.sql.parser.impl.CalciteTestSqlParserImpl;
import com.flink.calcite.sql.sqlnode.CustomSqlSelectEmit;
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
    /**
     * 自定义一个sql的逻辑
     * 1：编写 CustomSqlSubmit 继承SqlNode
     * 2：在Parser.tdd 中imports 添加类路径，和关键字
     * 3：在Parser.tdd 中statementParserMethods添加 CustomSqlSubmit()
     * 3：parserImpls.ftl 中添加 SQL语法
     * 4： <SUBMIT> <JOB> <AS>
     * @param args
     */
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
        String create = "CREATE TABLE $tableName (\n" +
                "         topic VARCHAR METADATA FROM 'topic',\n" +
                "          `offset` bigint METADATA,\n" +
                "          rowtime TIMESTAMP(3),\n" +
                "          msg VARCHAR,\n" +
                "          WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND\n" +
                "       ) WITH (\n" +
                "          'connector' = 'kafka',\n" +
                "          'topic' = '$topic',\n" +
                "          'scan.startup.mode' = 'latest-offset',\n" +
                "          'properties.bootstrap.servers' = '$broker',\n" +
                "          'properties.group.id' = '$groupID',\n" +
                "          'format' = '$format'\n" +
                "       )";
        String sql = "(select TUMBLE_START(rowtime, INTERVAL '3' SECOND) as TUMBLE_START," +
                "TUMBLE_END(rowtime, INTERVAL '3' SECOND) as TUMBLE_END," +
                "TUMBLE_ROWTIME(rowtime, INTERVAL '3' SECOND) as new_rowtime," +
                "msg,count(1) cnt from test group by TUMBLE(rowtime, INTERVAL '3' SECOND), msg)" +
                "EMIT WITH '10' SECOND";
        SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        try {
            CustomSqlSelectEmit sqlNode = (CustomSqlSelectEmit)parser.parseStmt();

            System.out.println(sqlNode.query.getKind());

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
