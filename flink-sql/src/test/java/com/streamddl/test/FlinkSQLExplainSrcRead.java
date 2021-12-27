package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.types.Row;
import org.junit.Test;

/**
 * 用于SQL 解析的源码走读
 */
public class FlinkSQLExplainSrcRead extends FlinkJavaStreamTableTestBase {
    @Test
    public void wordCount() throws Exception {
        // {"rowtime":"2020-01-01 00:00:01","msg":"c"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        String selectSQL = "SELECT msg,count(1) from test group by msg";
        System.out.println(tableEnv.explainSql(selectSQL));
        tableEnv.toRetractStream(tableEnv.sqlQuery(selectSQL), Row.class);
        streamEnv.execute("");
    }

    @Test
    public void sqlExplainTest() {
        StatementSet set = tableEnv.createStatementSet();
        // 设置 动态 option设置
        tableEnv.getConfig().getConfiguration()
                .set(ConfigOptions.key("table.dynamic-table-options.enabled")
                .booleanType().defaultValue(true), true);

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test1",
                        "test",
                        "json"));

        tableEnv.executeSql(DDLSourceSQLManager.createFromMysql("mysqltest"));
        String selectSQL =  " select a.uid,b.topic from" +
                " test a " +
                " left join " +
                " test1 b " +
                " on a.msg = b.uid" +
                "";

        System.out.println(tableEnv.explainSql(selectSQL));
//        set.addInsertSql("insert into mysqltest select msg,count(1) cnt from test group by msg");
//        set.execute();
//        TableResult re = tableEnv.executeSql("insert into mysqltest select msg,count(1) cnt from test group by msg");
//        re.print();
    }

    // 时态表
    @Test
    public void temporalTableTest() throws Exception {
        // {"rowtime":"2021-01-20 00:00:23","msg":"hello,world"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        // lookup只能支持 FOR SYSTEM_TIME AS OF t1.proctime 的方式join
        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
        String selectSQL = "select * from" +
                " test t1" +
                " LEFT JOIN" +
                " hbaselookup FOR SYSTEM_TIME AS OF t1.proctime as t2" +
                " ON t1.msg = t2.word";
        System.out.println(tableEnv.explainSql(selectSQL));

//        tableEnv.toAppendStream(
//                tableEnv.sqlQuery("" +
//                        "select * from test t1" +
//                        " JOIN hbaselookup FOR SYSTEM_TIME AS OF t1.proctime as t2" +
//                        " ON t1.msg = t2.word" +
//                ""), Row.class).print();
//        streamEnv.execute();
//        streamEnv.execute();
    }

    /**
     * local agg 依赖于 mini batch
     * 执行计划在 StreamPhysicalAggRule那再转一次，但是加上了localAgg
     */
    @Test
    public void aggOptimizePlan(){
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
        configuration.setString("table.exec.mini-batch.allow-latency", "5 s"); // use 5 seconds to buffer input records
        configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString("table.optimizer.agg-phase-strategy", "TWO_PHASE"); // enable two-phase, i.e. local-global aggregation

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));

        String selectSQL = "select msg,count(1) from test group by msg";
        System.out.println(tableEnv.explainSql(selectSQL));
    }

    @Test
    public void windowTest() throws Exception {
// {"rowtime":"2020-01-01 00:00:01","msg":"c"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        String selectSQL = "SELECT window_start,window_end,msg,COUNT(1) AS sellCount " +
                "FROM TABLE( TUMBLE(TABLE test, DESCRIPTOR(proctime), INTERVAL '10' SECONDS) )\n" +
                "GROUP BY window_start,window_end,msg";
        System.out.println(tableEnv.explainSql(selectSQL));
        tableEnv.toDataStream(tableEnv.sqlQuery(selectSQL), Row.class);

        streamEnv.execute("");
//        Calc(select=[window_start, window_end, msg, sellCount])
//        +- WindowAggregate(groupBy=[msg], window=[TUMBLE(time_col=[proctime], size=[10 s])], select=[msg, COUNT(*) AS sellCount, start('w$) AS window_start, end('w$) AS window_end])
//        +- Exchange(distribution=[hash[msg]])
//        +- Calc(select=[msg, PROCTIME() AS proctime])
//        +- TableSourceScan(table=[[default_catalog, default_database, test, watermark=[-($0, 10000:INTERVAL SECOND)]]], fields=[rowtime, msg, uid, topic, offset])
    }

}
