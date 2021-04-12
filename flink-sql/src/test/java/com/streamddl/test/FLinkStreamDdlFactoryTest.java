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
    public void elasticsearchSinkTest() {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        System.out.println(DDLSourceSQLManager.createCustomESSink("essinktest"));
        tableEnv.executeSql(DDLSourceSQLManager.createCustomESSink("essinktest"));
        TableResult re = tableEnv.executeSql("insert into essinktest" +
                " select " +
                "CONCAT_WS(',',msg) as id," +
                "msg," +
                "count(1) uv " +
                "from test" +
                " group by" +
                " msg," +
                " TUMBLE(proctime, INTERVAL '10' SECOND)");
//        TableResult re = tableEnv.executeSql("insert into mongotest select msg as id,msg,count(1) uv from test3 group by msg");
        re.print();
    }

    // 时态表
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
    // 不能在ddl中定义时态表函数
    // 时态表函数和时态表 DDL 最大的区别在于，时态表 DDL 可以在纯 SQL 环境中使用但是时态表函数不支持，用时态表 DDL 声明的时态表支持 changelog 流和 append-only 流但时态表函数仅支持 append-only 流。
    @Test
    public void temporalTableFunctionTest() throws Exception {
        // test: {"rowtime":"2021-01-20 00:00:24","msg":"hello"}
        // test2: {"rowtime":"2021-01-20 00:00:00","msg":"hello"}
        // 输出 00匹配结果
        // test2: {"rowtime":"2021-01-20 00:00:11","msg":"hello"}
        // test : {"rowtime":"2021-01-20 00:00:24","msg":"hello"}
        // 输出 11匹配结果
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


        // 时态表函数
        TemporalTableFunction rates = tableEnv.from("test2")
                .renameColumns($("msg").as("r_msg"),
                        $("proctime").as("r_proctime"))
                .createTemporalTableFunction(
                        $("r_proctime"),
                        $("r_msg"));

        tableEnv.createTemporarySystemFunction("rates", rates);

        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test," +
                " LATERAL TABLE(rates(proctime))" +
                " WHERE " +
                "  msg = r_msg" +
                ""), Row.class).print();
        streamEnv.execute();

    }

//注意 理论上讲任意都能用作时态表并在基于处理时间的时态表
// Join 中使用，但当前支持作为时态表的普通表必须实现接口 LookupableTableSource。
// 接口 LookupableTableSource 的实例只能作为时态表用于基于处理时间的时态 Join 。
    // 时态表-lookup表
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
