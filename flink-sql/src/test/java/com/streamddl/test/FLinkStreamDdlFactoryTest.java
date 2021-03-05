package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.sql.func.StrSplitTableFunction;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.func.dynamicfunc.source.tablefunc.HbaseTableFunc;
import com.func.udffunc.HbaseLookupFunction;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FLinkStreamDdlFactoryTest extends FlinkJavaStreamTableTestBase {


    @Test
    public void writetoJdbcTest() {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        System.out.println(DDLSourceSQLManager.createFromMysql("mysqltest"));
        tableEnv.executeSql(DDLSourceSQLManager.createFromMysql("mysqltest"));

        TableResult re = tableEnv.executeSql("insert into mysqltest select msg,count(1) cnt from test group by msg");
        re.print();
    }

    @Test
    public void mongoTest() {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test;test2",
                        "test",
                        "test",
                        "json"));

        System.out.println(DDLSourceSQLManager.createCustomMongoSink("mongotest"));
        tableEnv.executeSql(DDLSourceSQLManager.createCustomMongoSink("mongotest"));
        TableResult re = tableEnv.executeSql("insert into mongotest" +
                " select " +
                "'id' as id," +
                "msg," +
                "count(1) uv " +
                "from test" +
                " group by" +
                " msg," +
                " TUMBLE(proctime, INTERVAL '10' SECOND)");
//        TableResult re = tableEnv.executeSql("insert into mongotest select msg as id,msg,count(1) uv from test3 group by msg");
        re.print();
    }


    @Test
    public void temporalTableTest() throws Exception {
        // {"rowtime":"2021-01-20 00:00:23","msg":"hello,world"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test;test2",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
//        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from" +
//                " test,LATERAL TABLE(hbaselookup(msg))" +
//                " where msg = word" +
//                ""), Row.class).print();

        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test t1 JOIN hbaselookup FOR SYSTEM_TIME AS OF t1.proctime as t2 ON t1.msg = t2.word" +
                ""), Row.class).print();
        streamEnv.execute();
    }

    // 不能在ddl中定义时态表函数
    @Test
    public void UDTFTest() throws Exception {
        // {"rowtime":"2021-01-20 00:00:23","msg":"hello,world"}

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test;test2",
                        "test",
                        "test",
                        "json"));
        tableEnv.createTemporaryFunction("split", new StrSplitTableFunction(","));


        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test," +
                " LATERAL TABLE(split(msg)) AS t2(word, word2)" +
                ""), Row.class).print();


        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from" +
                " test LEFT JOIN" +
                " LATERAL TABLE(split(msg)) AS t(word, word1) ON TRUE"), Row.class).print();

        streamEnv.execute();

    }


    // 时态表函数
    @Test
    public void temporalTableFunctionTest() throws Exception {
        // {"rowtime":"2021-01-20 00:00:23","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test2",
                        "test2",
                        "test",
                        "json"));


        TemporalTableFunction rates = tableEnv.from("test2")
                .renameColumns($("msg").as("r_msg"),
                        $("proctime").as("r_proctime")).createTemporalTableFunction(
                        $("r_proctime"),
                        $("r_msg"));

        tableEnv.createTemporarySystemFunction("rates", rates);

        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test," +
                " LATERAL TABLE(rates(proctime))"+
                " WHERE " +
                "  msg = r_msg" +
                ""), Row.class).print();
        streamEnv.execute();

    }



    // 时态表
    @Test
    public void lookupTableTest() throws Exception {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test t1 JOIN" +
                " hbaselookup FOR SYSTEM_TIME AS OF t1.proctime as t2 ON t1.msg = t2.word" +
                ""), Row.class).print();
        streamEnv.execute();
    }


}
