package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class FlinkUpsertDDLTest extends FlinkJavaStreamTableTestBase {
    @Test
    public void upsertKafkaTest() throws ExecutionException, InterruptedException {
        tableEnv.executeSql(DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                "test",
                "test",
                "test",
                "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createUpsertKafkaSinkTable("localhost:9092",
                "test2",
                "test2",
                "test",
                "json"));
       String selectSQL = "SELECT msg,count(1) from test group by msg";
       StatementSet set = tableEnv.createStatementSet();
       set.addInsert("test2", tableEnv.sqlQuery(selectSQL));
        TableResult re = set.execute();
        re.await();
    }
}
