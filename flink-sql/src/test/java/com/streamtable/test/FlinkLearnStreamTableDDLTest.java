package com.streamtable.test;

import com.flink.common.java.pojo.TestPoJo;
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

    /**
     * 双流连接
     * @throws Exception
     */
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
    /**
     * 自定义的sinkfactory
     * 百度SPI
     *  要一次创建 META-INF 再创建services
     * 1： 需要再resources/META-INF.services/下创建一个接口名-(org.apache.flink.table.factories.TableFactory)的SPI文件（不是txt）
     * 2：在文件里面写上自己实现的类路径
     * 3：实现PrintlnAppendStreamFactory。
     */
    @Test
    public void testcustomSinkFactory() throws Exception {

        // ddl source
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka_CSV("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test2",
                        ",",
                        "test"));
        tableEnv.createTemporaryView("test" , tableEnv.from("test2")
                .renameColumns("id as topic")
                .renameColumns("name as msg")
                .renameColumns("age as ll"));
// sourve
//        Table a = getStreamTable(
//                getKafkaDataStream("test", "localhost:9092", "latest"),
//                "topic,offset,msg")
//                .renameColumns("offset as ll"); // offset是关键字
        //   tableEnv.createTemporaryView("test", a);


        tableEnv.executeSql(DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl("printlnSinkTbl"));

        DataStream b =
                tableEnv.toRetractStream(
                        tableEnv.sqlQuery("select topic,msg,count(1) as ll from test group by topic,msg")
                        , Row.class)
                        .filter(x -> x.f0)
                        .map(x -> new TestPoJo(x.f1.getField(0).toString(), x.f1.getField(1).toString(), Long.valueOf(x.f1.getField(2).toString())))
                        .returns(Types.POJO(TestPoJo.class));

        tableEnv.createTemporaryView("tmptale", tableEnv.fromDataStream(b));

        // 只能tableEnv.execute("");
        tableEnv.sqlQuery("select topic,msg,ll from tmptale")
                .insertInto("printlnSinkTbl");
        // 只能streamEnv.execute("");
        // tableEnv.toRetractStream(tableEnv.from("test"), Row.class).print();


        tableEnv.execute("");
    }


}
