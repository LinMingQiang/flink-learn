package com.streamhive.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.junit.Test;

public class StreamDataWriteToHiveTest extends FlinkJavaStreamTableTestBase {
    @Test
    public void testHiveCatalog() {
        // {"rowtime":"2021-01-20 20:01:23","msg":"hello"}
        HiveCatalog catalog = new HiveCatalog(
                "catalogName",              // catalog name
                "default",                // default database
                "/Users/eminem/workspace/flink/flink-learn/resources/conf",  // Hive config (hive-site.xml) directory
                "2.3.2"                   // Hive version
        );
        String sql = DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                "test",
                "test",
                "test",
                "json");
        System.out.println(sql);
        tableEnv.executeSql(
                sql);


        tableEnv.registerCatalog("catalogName", catalog);
        tableEnv.useCatalog("catalogName");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        TableResult r = tableEnv.executeSql("INSERT INTO TABLE hive_table SELECT msg as user_id, 1.1 as order_amount, DATE_FORMAT(rowtime, 'yyyy-MM-dd'), DATE_FORMAT(rowtime, 'HH') FROM test");
        r.print();
//        CREATE TABLE hive_table (
//                user_id STRING,
//                order_amount DOUBLE
//        ) PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (
//                'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',
//                'sink.partition-commit.delay'='1 s',
//                'sink.partition-commit.policy.kind'='metastore,success-file'
//);


        // SET table.sql-dialect=default;
//        CREATE TABLE test (
//        rowtime TIMESTAMP(3),
//                msg VARCHAR,
//                WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND
//) WITH (
//                'connector' = 'kafka',
//                'topic' = 'test',
//                'scan.startup.mode' = 'latest-offset',
//                'properties.bootstrap.servers' = 'localhost:9092',
//                'properties.group.id' = 'test',
//                'format' = 'json'
//        );
        //SET table.sql-dialect=hive;
//        INSERT INTO TABLE hive_table SELECT 'hello' as user_id, 1.1 as order_amount, DATE_FORMAT(rowtime, 'yyyy-MM-dd'), DATE_FORMAT(rowtime, 'HH') FROM test;

    }
}
