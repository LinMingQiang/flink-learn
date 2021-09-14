package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.learn.sql.func.HyperLogCountDistinctAgg;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkStreamTableDdlTest extends FlinkJavaStreamTableTestBase {

    /**
     * 在main里面可以执行，不需要 execute
     * 在test里面执行得用TableResult
     *
     * @throws Exception
     */
    @Test
    public void testDDLSample() throws Exception {
        // create table 不会触发任务,只有insert会
        // {"rowtime":"2021-01-20 00:00:23","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        tableEnv.toRetractStream(tableEnv.sqlQuery("select concat(msg, topic) as ss,count(*) as cnt from test group by msg,topic"), Row.class)
                .print();

        // 要在 executeSql 之前
        streamEnv.executeAsync();
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink select msg,count(*) as cnt from test group by msg");
        re.print();
    }


    // window的source表不能是 update表，
    // union的话是需要两边的wtm都达到才可以触发
    @Test
    public void testDDLWindow() throws Exception {
        // {"rowtime":"2021-01-20 00:00:01","msg":"hello"} {"rowtime":"2021-01-20 00:00:02","msg":"hello"}
        // {"rowtime":"2021-01-20 00:02:50","msg":"hello"}
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
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        // TUMBLE_ROWTIME 返回的字段做为 rowtime
//        " TUMBLE_START(rowtime, INTERVAL '3' SECOND) as TUMBLE_START," +
//                "TUMBLE_END(rowtime, INTERVAL '3' SECOND) as TUMBLE_END," +
//                "TUMBLE_ROWTIME(rowtime, INTERVAL '3' SECOND) as new_rowtime," +

        // select * from (select * from test) union all (select * from test)
        String sql = "select " +
                "msg," +
                "count(1) cnt" +
                " from test" +
                " where msg = 'hello' " +
                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
                "";
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink " + sql);
        re.print();

    }

    @Test
    public void testOverWindow() throws Exception {
        // {"rowtime":"2021-01-20 00:00:01","msg":"hello"}
        // {"rowtime":"2021-01-20 00:00:05","msg":"hello"}
        // {"rowtime":"2021-01-20 00:00:12","msg":"hello"} // 只触发第一条
        // {"rowtime":"2021-01-20 00:00:16","msg":"hello"}
        // {"rowtime":"2021-01-20 00:00:02","msg":"hello"} // 过期不参与计算
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        // TUMBLE_ROWTIME 返回的字段做为 rowtime
        String sql = "select count(distinct `offset`) over w,sum(`offset`) over w " +
                "FROM test " +
                "window w AS (" +
                "partition by msg " +
                "order by rowtime " +
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" +
                ")";
        tableEnv.toRetractStream(tableEnv.sqlQuery(sql), Row.class).print();

        streamEnv.execute();
    }

    @Test
    public void testIntervalJoin() throws Exception {
        // {"rowtime":"2021-01-20 00:00:13","msg":"hello"}
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
                        "test2",
                        "json"));
        // o-4 < s < o
        // o < s < o+4
        // o-1 < s < o+3
        // TUMBLE_ROWTIME 返回的字段做为 rowtime
        String sql = " SELECT o.* " +
                "FROM test o, test2 s " +
                "WHERE o.msg = s.msg AND" +
                "      o.rowtime BETWEEN s.rowtime - INTERVAL '4' SECOND AND s.rowtime";
        tableEnv.toRetractStream(tableEnv.sqlQuery(sql), Row.class).print();

        streamEnv.execute();
    }

    @Test
    public void joinTest() throws Exception {
        // {"rowtime":"2021-01-20 00:00:13","msg":"hello"}
        // join结果不能带上 rowtime。因为没用到rowtime所以不允许带
        //
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
                        "test2",
                        "json"));

        String sql = " SELECT o.msg,s.msg " +
                "FROM test o join test2 s " +
                "on o.msg = s.msg where s.msg='aa' and o.msg='aa'";
        System.out.println(tableEnv.explainSql(sql));;
        tableEnv.toRetractStream(tableEnv.sqlQuery(sql), Row.class).print();

        streamEnv.execute();
    }
    /**
     * 自定义的format
     *
     * @throws Exception
     */
    @Test
    public void testCustomJsonFormat() throws Exception {
        // {"rowtime":"2021-01-20 00:00:23","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka(
                        "localhost:9092",
                        "test",
                        "test",
                        "test",
                        "custom-json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        // TableResult re = tableEnv.executeSql("insert into printlnRetractSink select msg , 1 as cnt from test");
        TableResult re = tableEnv.executeSql("select msg , 1 as cnt from test");

        re.print();
    }


    @Test
    public void testDDLSample2() throws Exception {
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
                        "test2",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));

        String sql = "select msg, count(1) over w as w_c,sum(`offset`) over w as w_s " +
                "FROM test " +
                "window w AS (" +
                "partition by msg " +
                "order by rowtime " +
                "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" +
                ")";
        tableEnv.createTemporaryView("over_test", tableEnv.sqlQuery(sql));
        String sql2 = "select " +
                "msg," +
                "count(1) cnt" +
                " from test2" +
                " where msg = 'hello' " +
                " group by TUMBLE(rowtime, INTERVAL '3' SECOND), msg " +
                "";
        tableEnv.createTemporaryView("window_test", tableEnv.sqlQuery(sql2));

        String sql3 = "select a.*,b.* from window_test a join over_test b on a.msg = b.msg";

        tableEnv.toRetractStream(tableEnv.sqlQuery(sql3), Row.class).print();

        System.out.println(streamEnv.getExecutionPlan());
//        String sa = tableEnv.sqlQuery(sql).explain();
//        String sa2 = tableEnv.sqlQuery(sql2).explain();
//        System.out.println(sa);
//        System.out.println(sa2);

//        TableResult re = tableEnv.executeSql("insert into printlnRetractSink select msg,count(*) as cnt from test group by msg");
        // 想要输出得
//        re.print();

    }


    /**
     * 需要再sql的 WindowOperator 里面去修改
     * 必须是insert xx select... emit
     *
     * @throws Exception
     */
    @Test
    public void testDDLTriggerWindow() throws Exception {
        // {"rowtime":"2021-01-20 00:00:00","msg":"hello"} {"rowtime":"2021-01-20 00:00:02","msg":"hello"}
        // {"rowtime":"2021-01-20 00:02:50","msg":"hello"}
//        tableEnv.getConfig().getConfiguration().setBoolean("table.exec.emit.early-fire.enabled", true);
//        // 每隔 5s 触发一次
//        tableEnv.getConfig().getConfiguration().setLong("table.exec.emit.early-fire.delay", 5000L);
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        // TUMBLE_ROWTIME 返回的字段做为 rowtime
        String sql = "select " +
                "CONCAT_WS('--',cast(TUMBLE_START(rowtime, INTERVAL '30' SECOND) as VARCHAR)," +
                "cast(TUMBLE_END(rowtime, INTERVAL '30' SECOND)  as VARCHAR)," +
                "msg) as msg," +
                "count(1) cnt" +
                " from test" +
                " where msg = 'hello' " +
                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
                " EMIT \n" +
                "  WITH DELAY '2' SECOND BEFORE WATERMARK";
        // "  WITH DELAY '2' SECOND AFTER WATERMARK";

        String sql2 = "select " +
                "CONCAT_WS('--',TUMBLE_START(rowtime, INTERVAL '30' SECOND)," +
                "TUMBLE_END(rowtime, INTERVAL '30' SECOND)," +
                "msg) as msg," +
                "count(1) cnt" +
                " from test" +
                " where msg = 'hello' " +
                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
                " EMIT \n" +
                "  WITH DELAY '10' SECOND BEFORE WATERMARK,\n" +
                "  WITHOUT DELAY AFTER WATERMARK";

        // TableResult re2 = tableEnv.executeSql("insert into printlnRetractSink " + sql2);
        // re2.print();
//        tableEnv.toRetractStream(tableEnv.sqlQuery(sql), Row.class).print();
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink " + sql);
        re.print();
        streamEnv.execute();
    }


    @Test
    public void hyperlogCountdistinctTest() throws Exception {
        // {"rowtime":"2021-01-20 00:00:00","msg":"hello100"}
        // {"rowtime":"2021-01-20 00:00:44","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.createTemporarySystemFunction("hyperCountDistinct", new HyperLogCountDistinctAgg());
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
//        String sql = "select " +
//                "msg," +
//                "hyperCountDistinct(msg) cnt" +
//                " from test " +
//                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
//                "";
        String sql = "select " +
                "msg," +
                "count(distinct msg) cnt" +
                " from test " +
                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
                "";
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink " + sql);
        re.print();

    }


    /**
     * datastream转table的event time问题
     * @throws Exception
     */
    @Test
    public void streamToTableTimeAttributesTest() throws Exception {
        // {"rowtime":"2021-01-20 01:00:00","msg":"hello"}
        // {"rowtime":"2021-01-20 01:00:20","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));

        // 正常触发
              tableEnv.createTemporaryView("test2", tableEnv.sqlQuery("select CONCAT(msg , '-hai') as msg,rowtime from test where msg is not null"));
//              tableEnv.createTemporaryView("test2", tableEnv.sqlQuery("select msg,rowtime from (select msg,rowtime from test) union all (select msg,rowtime from test)"));
        // 无法触发
//            tableEnv.createTemporaryView("test2", tableEnv.sqlQuery("select msg,rowtime from test group by msg,rowtime"));
        // 1: table转stream。同时指定 wtm的时间抽取
        SingleOutputStreamOperator r = tableEnv.toRetractStream(tableEnv.from("test2"), Row.class)
                .filter(x -> x.f0)
                // 加了keyby就无法触发
//                .keyBy((KeySelector<Tuple2<Boolean, Row>, String>) value -> value.f1.getField(0).toString())
                .map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String, String>>() {
                    SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    @Override
                    public Tuple2<String, String> map(Tuple2<Boolean, Row> value) throws Exception {
                        String formatstr = value.f1.getField(1).toString();
                        if (formatstr.length() < 19) formatstr += ":00";
                        return new Tuple2<>(value.f1.getField(0).toString(),
                                formatstr
                        );
                    }
                })
                // 这个wtm的定义可以在.watermark("rowtime", "rowtime - INTERVAL '10' SECOND") 定义了，1.13版本
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
//                                .withTimestampAssigner(((element, recordTimestamp) -> element.f1))
//                )
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                ;
// 1.13的方式定义 wtm和rowtime
        tableEnv.createTemporaryView("test3", tableEnv.fromDataStream(r, Schema.newBuilder()
                .columnByMetadata("rowtime", "TIMESTAMP(3)")
                .column("f0", "VARCHAR")
                .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
                .build())
                .renameColumns($("f0").as("msg")));
//        tableEnv.createTemporaryView("test3", r, $("msg"), $("rowtime").rowtime());

        String sql = "select " +
                "msg," +
                "count(1) cnt" +
                " from test3 " +
                " group by TUMBLE(rowtime, INTERVAL '10' SECOND), msg " +
                "";
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink " + sql);
        re.print();
    }

    /**
     * 测试当数据源做union的时候wtm生成
     * 结果：选的是两个stream最小的做wtm
     */
    @Test
    public void unionWatermarkTest(){
        // {"rowtime":"2021-01-20 01:00:00","msg":"hello"}
        // {"rowtime":"2021-01-20 01:02:00","msg":"hello"}
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
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));

        String unionAll = "CREATE VIEW uniontabl AS " +
                "select * from " +
                "(select msg,rowtime from test) union all (select msg,rowtime from test2)";
        tableEnv.executeSql(unionAll);
        String sql = "select " +
                "msg," +
                "count(1) cnt" +
                " from uniontabl " +
                " group by TUMBLE(rowtime, INTERVAL '10' SECOND), msg " +
                "";
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink " + sql);
        re.print();

    }



    @Test
    public void windowTVF(){
        // {"rowtime":"2021-01-20 01:00:02","msg":"hello","uid":"2"}
       /// {"rowtime":"2021-01-20 01:00:02","msg":"hello","uid":"2"}
        // {"rowtime":"2021-01-20 01:00:02","msg":"hello","uid":"2"}
        // {"rowtime":"2021-01-20 01:00:41","msg":"hello","uid":"1"}
        // {"rowtime":"2021-01-20 01:00:41","msg":"hello","uid":"1"}
        // {"rowtime":"2021-01-20 01:00:41","msg":"hello","uid":"1"}
        // {"rowtime":"2021-01-20 01:01:21","msg":"hello","uid":"3"}
        // {"rowtime":"2021-01-20 02:03:01","msg":"hello","uid":"4"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));

//        String sql = "select * " +
//                " from TABLE(TUMBLE(TABLE test,DESCRIPTOR(rowtime), INTERVAL '30' SECOND))";
//        tableEnv.sqlQuery(sql).printSchema();
        // window_start,window_end这个是必须的


//        String sql = "select " +
//                "msg," +
//                "count(1) cnt" +
//                " from TABLE(HOP(TABLE test, DESCRIPTOR(rowtime), INTERVAL '5' SECOND, INTERVAL '10' SECOND))" +
//                " group by window_start,window_end,msg " +
//                "";

                String sql = "select " +
                "msg," +
                "count(1) cnt" +
                " from TABLE(TUMBLE(TABLE test,DESCRIPTOR(rowtime), INTERVAL '30' SECOND))" +
                " group by window_start,window_end,msg " +
                "";

        // CUMULATE 函数，每隔 30s计算一次 当天的count, 每个30s窗口都会输出一次
        // 滑动窗口实现的，可以解决数据跳变问题
//        String sql = "select " +
//                "msg," +
//                "count(uid) cnt" +
//                " from TABLE(CUMULATE(TABLE test,DESCRIPTOR(rowtime), INTERVAL '30' SECOND, INTERVAL '1' DAY))" +
//                " group by window_start,window_end,msg " +
//                "";

        TableResult re = tableEnv.executeSql("insert into printlnRetractSink " + sql);
        re.print();
    }


}
