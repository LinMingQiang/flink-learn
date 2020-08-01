package com.streamtable.test;

import com.flink.learn.sql.common.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkLearnStreamTableDDLTest extends FlinkJavaStreamTableTestBase {
    @Test
    public void testDDLSample() throws Exception {
        tableEnv.sqlUpdate(
                DDLSourceSQLManager.createStreamFromKafka_CSV("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ",",
                        "test"));
        tableEnv.toRetractStream(tableEnv
                .sqlQuery("select id,count(*) num from test group by id"), Row.class)
                .filter(x -> x.f0)
                .map(x -> x.f1)
                .print();
        // insertIntoCsvTbl(tEnv)
        tableEnv.execute("FlinkLearnStreamDDLSQLEntry");
    }


    @Test
    public void testDDLWindow(){

    }
}
