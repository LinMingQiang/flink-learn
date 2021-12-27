package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class FlinkUpsertDDLTest extends FlinkJavaStreamTableTestBase {
    /**
     * kafka 和 jdbc 一样写 upsert 出去的时候只写 insert 和 update_after ，对 delete会将value设为null发出
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void upsertKafkaTest() throws ExecutionException, InterruptedException {
        tableEnv.executeSql(DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                "test",
                "test",
                "test",
                "json"));
        // kafka的upsert输出同正常没什么两样，一个区别是： 更新模式有 I U D，delete的数据会发送一条null数据
        tableEnv.executeSql(DDLSourceSQLManager.createUpsertKafkaSinkTable("localhost:9092",
                "test2",
                "test2",
                "test",
                "json"));
//        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("test2"));

       String selectSQL = "SELECT msg,count(1) from test group by msg";
       StatementSet set = tableEnv.createStatementSet();
       set.addInsert("test2", tableEnv.sqlQuery(selectSQL));
        TableResult re = set.execute();
        re.await();
    }


    /**
     * mysql -uroot -p123456
     * CREATE TABLE t1(
     *     msg char(255) not null primary key,
     *     cnt BIGINT(20)
     * );
     * sink的时候配置了批次和间隔后，对数据的缓存是按key去重之后最后再写出，同一个key只保留最新的记录
     */
    @Test
    public void upsertMysql() throws ExecutionException, InterruptedException {
        tableEnv.executeSql(DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                "test",
                "test",
                "test",
                "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createUpsertMysqlSinkTable("mysqlSinkTbl"));
        String selectSQL = "SELECT msg,count(1) cnt from test group by msg";
        StatementSet set = tableEnv.createStatementSet();
        set.addInsert("mysqlSinkTbl", tableEnv.sqlQuery(selectSQL));
        TableResult re = set.execute();
        re.await();
    }




}
