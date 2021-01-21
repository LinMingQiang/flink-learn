package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.common.java.pojo.TestPoJo;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
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
        // {"rowtime":"2021-01-20 00:00:13","msg":"hello"}
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        // TUMBLE_ROWTIME 返回的字段做为 rowtime
        String sql = "select TUMBLE_START(rowtime, INTERVAL '3' SECOND) as TUMBLE_START," +
                "TUMBLE_END(rowtime, INTERVAL '3' SECOND) as TUMBLE_END," +
                "TUMBLE_ROWTIME(rowtime, INTERVAL '3' SECOND) as new_rowtime," +
                "msg,count(1) cnt from test group by TUMBLE(rowtime, INTERVAL '3' SECOND), msg";
        tableEnv.toRetractStream(tableEnv.sqlQuery(sql), Row.class).print();

        streamEnv.execute();
    }


    @Test
    public void testIntervalJoin() throws Exception {
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
        TableResult re = tableEnv.executeSql("insert into printlnRetractSink select msg , 1 as cnt from test");

        re.print();
    }
}
