package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

public class FlinkUpsertDDLTest extends FlinkJavaStreamTableTestBase {
    /**
     * kafka 和 jdbc 一样写 upsert 出去的时候只写 insert 和 update_after ，对 delete会将value设为null发出
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void upsertKafkaSinkTest() throws ExecutionException, InterruptedException {
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
//     tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("test2"));

       String selectSQL = "SELECT msg,count(1) from test group by msg";
       StatementSet set = tableEnv.createStatementSet();
       set.addInsert("test2", tableEnv.sqlQuery(selectSQL));
        TableResult re = set.execute();
        re.await();
    }
    /**
     * upsert-kafka 只支持从 earliest开始消费 （其实输入数据都是 I和D）
     * // 查看执行计划就能看懂了，多了一个 ChangelogNormalize 用来处理输入的数据，
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void upsertKafkaSourceTest() throws Exception {
        // {"msg":"c","cnt":10}
        // 会把数据做state，输出为 +U -U
        tableEnv.executeSql(DDLSourceSQLManager.createUpsertKafkaSinkTable("localhost:9092",
                "test2",
                "test",
                "test",
                "json"));
     tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("test2"));
        System.out.println(tableEnv.explainSql("insert into test2 select * from test"));
        StatementSet set = tableEnv.createStatementSet();
        set.addInsert("test2", tableEnv.from("test"));
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
     * {"rowtime":"2020-01-01 00:00:01","msg":"c"}
     */
    @Test
    public void upsertMysql() throws ExecutionException, InterruptedException {
        tableEnv.executeSql(DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                "test",
                "test",
                "test",
                "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createUpsertMysqlSinkTable("mysqlSinkTbl"));
//        String selectSQL = "SELECT msg,count(1) cnt from test group by msg";

        System.out.println(tableEnv.explainSql("insert into mysqlSinkTbl SELECT msg,count(1) cnt from test group by msg"));

        StatementSet set = tableEnv.createStatementSet();
        set.addInsertSql("insert into mysqlSinkTbl SELECT msg,count(1) cnt from test group by msg");
        TableResult re = set.execute();
        re.await();
    }




}
