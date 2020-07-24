package com.flink.java.test;

import com.flink.common.java.test.FlinkJavaStreamTableTestBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkStreamTest extends FlinkJavaStreamTableTestBase {
    @Test
    public void test(){

    }

    /**
     * table 转stream
     *
     * @throws Exception
     */
    @Test
    public void testTableToStream() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(
                        getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg");
        a.printSchema();
        tableEnv.createTemporaryView("test", a);
        // 具有group by 需要用到state 用 toRetractStream
        tableEnv.toRetractStream(
                tableEnv.sqlQuery("select topic,count(1) from test group by topic"),
                Row.class).print();
        // 只追加数据，没有回溯历史数据可以用 append
        tableEnv.toAppendStream(
                tableEnv.sqlQuery("select * from test"),
                Row.class).print();
        tableEnv.execute("");
    }

}
