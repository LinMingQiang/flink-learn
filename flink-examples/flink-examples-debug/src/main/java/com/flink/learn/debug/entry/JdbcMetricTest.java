package com.flink.learn.debug.entry;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import com.ddlsql.DDLSourceSQLManager;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/** 测试. */
public class JdbcMetricTest {
    // flink run -c com.flink.learn.example.entry.Kafka2MysqlUpser
    // /Users/eminem/workspace/flink/flink-learn/flink-example/target/flink-example-1.13.3.jar
    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings sett = EnvironmentSettings.newInstance().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, sett);
        env.enableCheckpointing(10000L); // 更新offsets。每60s提交一次
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig()
                .enableExternalizedCheckpoints(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        RocksDBStateBackend rocksDBStateBackend =
                new RocksDBStateBackend(
                        "file:///Users/eminem/workspace/flink/flink-learn/checkpoint/WordCount",
                        false);
        rocksDBStateBackend.setDbStoragePath(
                "/Users/eminem/workspace/flink/flink-learn/checkpoint/rocksdb-tmp-file");
        env.setStateBackend(rocksDBStateBackend);

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka(
                        "localhost:9092", "test", "test", "test", "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createPrint("mysqlSinkTbl"));
        System.out.println(
                tableEnv.explainSql(
                        "insert into mysqlSinkTbl SELECT msg,count(1) cnt from test group by msg"));
        StatementSet set = tableEnv.createStatementSet();
        set.addInsertSql("insert into mysqlSinkTbl SELECT msg,count(1) cnt from test group by msg");
        set.execute().await();
    }
}
