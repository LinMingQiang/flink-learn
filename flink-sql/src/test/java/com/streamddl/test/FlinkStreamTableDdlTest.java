package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.common.java.pojo.TestPoJo;
import com.flink.learn.sql.func.HyperLogCountDistinctAgg;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkStreamTableDdlTest extends FlinkJavaStreamTableTestBase {

    /**
     * 在main里面可以执行，不需要 execute
     * 在test里面执行得用TableResult
     *
     * @throws Exception
     */
    @Test
    public void testDDLSample() throws Exception {
        // {"rowtime":"2021-01-20 00:00:23","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink select msg,count(*) as cnt from test group by msg");

        // 想要输出得
        re.print();

    }


    @Test
    public void testDDLWindow() throws Exception {
        // {"rowtime":"2021-01-20 00:00:01","msg":"hello"} {"rowtime":"2021-01-20 00:00:02","msg":"hello"}
        // {"rowtime":"2021-01-20 00:00:50","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        // TUMBLE_ROWTIME 返回的字段做为 rowtime
//        " TUMBLE_START(rowtime, INTERVAL '3' SECOND) as TUMBLE_START," +
//                "TUMBLE_END(rowtime, INTERVAL '3' SECOND) as TUMBLE_END," +
//                "TUMBLE_ROWTIME(rowtime, INTERVAL '3' SECOND) as new_rowtime," +
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
     * 使用了Emit，似乎不会清理窗口状态了，需要再sql的windowoperation里面去修改
     *
     * @throws Exception
     */
    @Test
    public void testDDLTriggerWindow() throws Exception {
        // {"rowtime":"2021-01-20 00:00:00","msg":"hello"} {"rowtime":"2021-01-20 00:00:02","msg":"hello"}
        // {"rowtime":"2021-01-20 00:00:40","msg":"hello"}
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
                "msg," +
                "count(1) cnt" +
                " from test" +
                " where msg = 'hello' " +
                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
                " EMIT \n" +
                "  WITH DELAY '2' SECOND BEFORE WATERMARK";
        // "  WITH DELAY '2' SECOND AFTER WATERMARK";

        String sql2 = "select " +
                "msg," +
                "count(1) cnt" +
                " from test" +
                " where msg = 'hello' " +
                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
                " EMIT \n" +
                "  WITH DELAY '10' SECOND BEFORE WATERMARK,\n" +
                "  WITHOUT DELAY AFTER WATERMARK";

        // TableResult re2 = tableEnv.executeSql("insert into printlnRetractSink " + sql2);
        // re2.print();

        TableResult re = tableEnv.executeSql("insert into printlnRetractSink " + sql);
        re.print();
    }


    @Test
    public void hyperlogCountdistinctTest() throws Exception {
        // {"rowtime":"2021-01-20 00:00:00","msg":"hello100"} {"rowtime":"2021-01-20 00:00:02","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.createTemporarySystemFunction("hyperCountDistinct", new HyperLogCountDistinctAgg());
        tableEnv.toRetractStream(tableEnv.sqlQuery(
                "select topic,hyperCountDistinct(msg) from test group by topic"), Row.class)
        .print();

        streamEnv.execute();


    }


}
