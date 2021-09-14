package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.learn.sql.func.StrSplitTableFunction;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.func.dynamicfunc.source.tablefunc.HbaseTableFunc;
import com.func.udffunc.HbaseLookupFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.Duration;

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


        System.out.println(tableEnv.explainSql("insert into mysqltest select msg,count(1) cnt from test group by msg"));
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
        // {"rowtime":"2020-06-18 16:59:30","msg":"1"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test t1 left JOIN" +
                " hbaselookup FOR SYSTEM_TIME AS OF t1.proctime as t2 ON t1.msg = t2.word" +
                ""), Row.class).print();
        streamEnv.execute();
    }

    // 以cdc做为维表进行join。cdc第一次会把所有数据都load一遍
    // {"rowtime":"2020-06-18 16:59:30","msg":"1"}
    // {"rowtime":"2020-06-18 16:59:30","msg":"1"}
    // 需要两边的wtm才能触发
    // 时态表分： 版本表和普通表
    // FlinkCDCTest
    // 版本表: 如果时态表中的记录可以追踪和并访问它的历史版本，这种表我们称之为版本表，来自数据库的 changelog 可以定义成版本表。1：使用cdc，或者debezium-json格式的kafka数据。
    // 普通表: 如果时态表中的记录仅仅可以追踪并和它的最新版本，这种表我们称之为普通表，来自数据库 或 HBase 的表可以定义成普通表。2：使用LookupableTableSource自己实现
    // 普通表的建表d：dl和正常表是一样的，理论上讲任意都能用作时态表并在基于处理时间的时态表 Join 中使用，
    // 但当前支持作为时态表的普通表必须实现接口 LookupableTableSource。
    // 接口 LookupableTableSource 的实例只能作为时态表用于基于处理时间的时态 Join 。
    // 版本表的建表：1：需要主键 PRIMARY KEY(product_id) NOT ENFORCED,
    //            2：需要事件事件：WATERMARK FOR update_time AS update_time

    // 这里是普通版的lookuptable
    @Test
    public void temporalTableJoinTest() throws Exception {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));

        // 版本表 kafka： 需要定义主键，需要时间，这些格式必须是 debezium-json ，json格式不支持主键的定义
        // 另一个版本表的定义是通过cdc。看 FlinkCDCTest::cdctemporalTableJoinTest
        tableEnv.executeSql(
                DDLSourceSQLManager.createTemporalTable("localhost:9092",
                        "test2",
                        "test2",
                        "test",
                        "debezium-json")); // json格式不支持 主键的定义,只能用 debezium-json，这个是cdc里面的
        // 普通表：lookup table .只支持proctime，
        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
        tableEnv.toRetractStream(tableEnv.sqlQuery("" +
                "select * from test as o JOIN " +
                " hbaselookup FOR SYSTEM_TIME AS OF o.proctime r" +
                " on o.msg = r.word" +
                ""), Row.class)
                .print();
        streamEnv.execute();

    }


    /**
     * 定义一个Row(xx Long, yy Int) 的类型，用来将指标统一在一个字段里面，方便 写出的时候用
     * {"rowtime":"2020-06-18 16:59:30","msg":"1"}
     */
    @Test
    public void sqlRowTypeTest() throws Exception {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        String sql = "select msg as id,msg,Row(count(1), sum(`offset`)) as inc from test group by msg";
//        tableEnv.sqlQuery(sql).printSchema();
//        for (Column column : tableEnv.sqlQuery(sql).getResolvedSchema().getColumns()) {
//            System.out.println(column.getName() + ":" +column.getDataType());
//        }
//        tableEnv.toRetractStream(tableEnv.sqlQuery(sql), Row.class)
//                .map(new MapFunction<Tuple2<Boolean, Row>, String>() {
//                    @Override
//                    public String map(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
//                        return booleanRowTuple2.toString();
//                    }
//                })
//                .print();

        tableEnv.executeSql(DDLSourceSQLManager.createCustomRowMongoSink("mongotest"));
        TableResult re = tableEnv.sqlQuery(sql).executeInsert("mongotest");
        re.print();
//        streamEnv.execute();
    }

    /**
     * 日志时间戳转TIMESTAMP
     * @throws Exception
     */
    @Test
    public void testBigintToTimeStamp3() throws Exception {
// {"rowtime":1626646800739,"msg":"hello,world"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafkaRowtime("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from test"), Row.class).print();
        streamEnv.execute();
    }
}
