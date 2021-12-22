package com.calcite.test;

import com.calcite.sql.parser.impl.CalciteTestSqlParserImpl;
import com.flink.calcite.sql.sqlnode.CustomSqlSelectEmit;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

public class EmitTest {
    /**
     * 自定义一个sql的逻辑
     * 1：编写 CustomSqlSelectEmit 继承 SqlNode
     * 2：在Parser.tdd 中 imports 添加类路径，和关键字
     * 3：在Parser.tdd 中 statementParserMethods添加 CustomSqlSelectEmit()
     * 4：parserImpls.ftl 中添加 SQL语法
     * 4：
     *
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
//        String create = "CREATE TABLE $tableName (\n" +
//                "         topic VARCHAR METADATA FROM 'topic',\n" +
//                "          `offset` bigint METADATA,\n" +
//                "          rowtime TIMESTAMP(3),\n" +
//                "          msg VARCHAR,\n" +
//                "          WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND\n" +
//                "       ) WITH (\n" +
//                "          'connector' = 'kafka',\n" +
//                "          'topic' = '$topic',\n" +
//                "          'scan.startup.mode' = 'latest-offset',\n" +
//                "          'properties.bootstrap.servers' = '$broker',\n" +
//                "          'properties.group.id' = '$groupID',\n" +
//                "          'format' = '$format'\n" +
//                "       )";

        String sql = "select * from test " +
                " EMIT \n" +
                "  WITH DELAY '1'MINUTE BEFORE WATERMARK,\n" +
                "  WITHOUT DELAY AFTER WATERMARK";

        SqlParser parser = SqlParser.create(sql, config.getParserConfig());
        try {

            System.out.println((CustomSqlSelectEmit) parser.parseStmt());
            // 阿里源码emit。是
//            RichSqlInsert sqlNode = (RichSqlInsert)parser.parseStmt();
//            CustomSqlSelectEmit emit = (CustomSqlSelectEmit)sqlNode.getSource();
//            System.out.println(emit.getEmit().getBeforeDelay());
            // RichSqlInsert对象。在源码里面做一个判断，拿出emit。然后设置conf
//            conf.setBoolean(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED, true)
//            conf.set(TABLE_EXEC_EMIT_LATE_FIRE_DELAY, Duration.ofSeconds(5))
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
