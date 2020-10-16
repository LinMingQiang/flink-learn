package com.streamtable.test;

import com.flink.learn.sql.common.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkLearnStreamTableDDLTest extends FlinkJavaStreamTableTestBase {
    @Test
    public void testDDLSample() throws Exception {
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka_CSV("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ",",
                        "test"));
        tableEnv.toRetractStream(tableEnv
                .sqlQuery("select id,count(*) num from test group by id"), Row.class)
                .filter(x -> x.f0)
                .map(x -> x.f1)
                .print();
        // insertIntoCsvTbl(tEnv)
        streamEnv.execute("FlinkLearnStreamDDLSQLEntry");
    }


    @Test
    public void testDDLWindow() {

    }

    @Test
    public void testStreamConneect() throws Exception {

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka_CSV("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ",",
                        "test"));
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka_CSV("localhost:9092",
                        "localhost:2181",
                        "test2",
                        "test2",
                        ",",
                        "test2"));

        tableEnv.executeSql(DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl("printlnSinkTbl"));

        DataStream<Row> a1 =
                tableEnv.toAppendStream(
                        tableEnv.sqlQuery("select topic,msg from test") , Row.class);
        DataStream<Row> a2 =
                tableEnv.toAppendStream(
                        tableEnv.sqlQuery("select topic,msg from test2") , Row.class);

        DataStream a3 = a1.connect(a2).map(new CoMapFunction<Row, Row, TestPoJo>() {
            @Override
            public TestPoJo map1(Row x) throws Exception {
                TestPoJo r = new TestPoJo(x.getField(0).toString(), x.getField(1).toString(), 1L);
                        return r;
            }
            @Override
            public TestPoJo map2(Row x) throws Exception {
                TestPoJo r = new TestPoJo(x.getField(0).toString(), x.getField(1).toString(), 1L);
                return r;
            }
        }).returns(Types.POJO(TestPoJo.class));

        tableEnv.createTemporaryView("tmptale1", tableEnv.fromDataStream(a3));
        tableEnv.sqlQuery("select topic,msg,sum(ll) ll from tmptale1 group by topic,msg")
                .insertInto("printlnSinkTbl");

        tableEnv.execute("");
    }


}
