package com.flink.java.sourcesink.test;

import com.flink.common.java.connect.PrintlnConnect;
import com.flink.common.java.sourcefunc.HbaseLookupFunction;
import com.flink.common.java.tablesource.HbaseAyscLookupTableSource;
import com.flink.common.manager.SchemaManager;
import com.flink.learn.sql.common.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils;
import org.apache.flink.addons.hbase.HBaseTableSource;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.csv.CsvRowFormatFactory;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.factories.StreamTableSourceFactory;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkStreamCoreSourceSinkTest extends FlinkJavaStreamTableTestBase {
    /**
     * 自定义的sinkfactory
     * 百度SPI
     * 要一次创建 META-INF 再创建services
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
        tableEnv.createTemporaryView("test", tableEnv.from("test2")
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

    /**
     * 功能同上
     *
     * @throws Exception
     */
    @Test
    public void testPrintlneConnect() throws Exception {
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg,pt.proctime")
                .renameColumns("offset as ll"); // offset是关键字
        tableEnv.createTemporaryView("test", a);
        tableEnv.toAppendStream(a, Row.class).print();
// append
//        tableEnv
//                .connect(new PrintlnConnect().property("println.prefix", "connect sink : "))
//                .inAppendMode()
//                .withFormat(ConnectorFormatDescriptorUtils.kafkaConnJsonFormat())
//                .withSchema(SchemaManager.PRINTLN_SCHEMA())
//                .createTemporaryTable("printlnSinkTbl");
//        tableEnv.insertInto("printlnSinkTbl", a.select("topic,msg,ll"));

        // retract
//        tableEnv
//                .connect(new PrintlnConnect("printsink_retract", 1, true))
//                .inRetractMode()
//                .withFormat(ConnectorFormatDescriptorUtils.kafkaConnJsonFormat())
//                .withSchema(SchemaManager.PRINTLN_SCHEMA())
//                .createTemporaryTable("printlnSinkTbl");
//
//        tableEnv.insertInto("printlnSinkTbl",
//                tableEnv.sqlQuery("select topic,msg,count(ll) as ll from test group by topic,msg"));

// upsert
//        tableEnv
//                .connect(new PrintlnConnect("printsink_upsert", 1, true))
//                .inRetractMode()
//                .withFormat(ConnectorFormatDescriptorUtils.kafkaConnJsonFormat())
//                .withSchema(SchemaManager.PRINTLN_SCHEMA())
//                .createTemporaryTable("printsink_upsert");
//
//        tableEnv.insertInto("printsink_upsert",
//                tableEnv.sqlQuery("select topic,msg,count(ll) as ll from test group by topic,msg"));
//
        tableEnv.execute("");
    }

    /**
     * hbase
     */
    @Test
    public void testLookupTableSource() throws Exception {
        // {"id":"id2","name":"name","age":1}
        tableEnv.sqlUpdate(
                DDLSourceSQLManager.createStreamFromKafkaProcessTime("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ","));
        // 方法1
        tableEnv.sqlUpdate(DDLSourceSQLManager.createHbaseLookupSourceTbl("hbaselookup"));
        tableEnv.toAppendStream(
                tableEnv.sqlQuery("select * from test as t left join" +
                        " hbaselookup FOR SYSTEM_TIME AS OF t.proctime AS hb" +
                        " on hb.id = t.id and t.name = hb.name"), Row.class)
                .print();

// 方法2
//        tableEnv.registerFunction("hbaselookup", HbaseLookupFunction.builder()
//                        .setFieldNames(schema.getFieldNames())
//                        .setFieldTypes(schema.getFieldTypes())
//                        .build());
//
//        tableEnv.toAppendStream(
//                tableEnv.sqlQuery("select * from test, LATERAL TABLE (hbaselookup(id, name))"), Row.class)
//                .print();

        streamEnv.execute("");
    }


    @Test
    public void testCustomJsonFormat() throws Exception {
        // {"id":"id2","name":"name}
        tableEnv.sqlUpdate(
                DDLSourceSQLManager.createStreamFromKafkaUseCustomFormat("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ","));

        tableEnv.toAppendStream(
                tableEnv.from("test"), Row.class)
                .print();

        streamEnv.execute("");
    }

    @Test
    public void testCSV() throws Exception {
        // {"id":"id2","name":"name}
        tableEnv.sqlUpdate(
                DDLSourceSQLManager.createStreamFromKafka_CSV("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ",",
                        "test"));

        tableEnv.toAppendStream(
                tableEnv.from("test"), Row.class)
                .print();

        streamEnv.execute("");
    }

    @Test
    public void testCustomCSV() throws Exception {
        // {"id":"id2","name":"name}
        tableEnv.sqlUpdate(
                DDLSourceSQLManager.createStreamFromKafka_CUSTOMCSV("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ",",
                        "test"));

        tableEnv.toAppendStream(
                tableEnv.from("test"), Row.class)
                .print();

        streamEnv.execute("");
    }


    @Test
    public void testTemporalTable() throws Exception {
        // {"id":"id2","name":"name}
        tableEnv.sqlUpdate(
                DDLSourceSQLManager.createStreamFromKafkaProcessTime("localhost:9092",
                        "localhost:2181",
                        "test",
                        "test",
                        ","));
        tableEnv.sqlUpdate(
                DDLSourceSQLManager.createStreamFromKafkaProcessTime("localhost:9092",
                        "localhost:2181",
                        "test2",
                        "test2",
                        ","));

        tableEnv.createTemporarySystemFunction("productInfoFunc",
                tableEnv.from("test2")
                .createTemporalTableFunction("proctime", "id"));


        tableEnv.toAppendStream(
                tableEnv.sqlQuery("select * from test"), Row.class)
                .print();
        tableEnv.toAppendStream(
                tableEnv.sqlQuery("select * from test2"), Row.class)
                .print();
        tableEnv.toAppendStream(
                tableEnv.sqlQuery("select productInfo.id,t.id,t.name from test as t," +
                        "LATERAL TABLE(productInfoFunc(t.proctime)) as productInfo " +
                        "WHERE t.id=productInfo.id"), Row.class)
                .print();

        streamEnv.execute("");
    }

}
