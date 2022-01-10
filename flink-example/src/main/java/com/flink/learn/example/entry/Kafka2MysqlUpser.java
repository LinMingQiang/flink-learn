package com.flink.learn.example.entry;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ddlsql.DDLSourceSQLManager;

import java.util.concurrent.ExecutionException;

/** 主要 实现的hll_distict需要有merge方法 */
public class Kafka2MysqlUpser {
    // flink run -c com.flink.learn.example.entry.Kafka2MysqlUpser
    // /Users/eminem/workspace/flink/flink-learn/flink-example/target/flink-example-1.13.3.jar
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings sett = EnvironmentSettings.newInstance().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, sett);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString(
                "table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
        configuration.setString(
                "table.exec.mini-batch.allow-latency",
                "5 s"); // use 5 seconds to buffer input records
        configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString(
                "table.optimizer.agg-phase-strategy",
                "TWO_PHASE"); // enable two-phase, i.e. local-global aggregation
        tableEnv.executeSql(
                "CREATE FUNCTION hll_distinct AS 'com.hll.FlinkUDAFCardinalityEstimationFunction'");

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka(
                        "localhost:9092", "test", "test", "test", "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createUpsertMysqlSinkTable("mysqlSinkTbl"));
        System.out.println(
                tableEnv.explainSql(
                        "insert into mysqlSinkTbl SELECT msg,count(distinct msg) cnt from test group by msg"));
        StatementSet set = tableEnv.createStatementSet();
        set.addInsertSql(
                "insert into mysqlSinkTbl SELECT msg,count(distinct msg) cnt from test group by msg");
        set.execute().await();
    }
}
