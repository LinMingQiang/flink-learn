package com.streamtable.test;

import com.flink.common.manager.SchemaManager;
import com.flink.common.manager.TableSinkManager;
import com.flink.common.manager.TableSourceConnectorManager;
import com.flink.learn.sql.common.DDLSourceSQLManager;
import com.flink.learn.sql.common.DDLSourceSQLManager$;
import com.flink.learn.test.common.FlinkStreamTableTestBase;
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ServiceLoader;

public class FlinkLearnStreamExcutionEntry extends FlinkStreamTableTestBase {
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

    @Test
    public void testStreamToTable() throws Exception {
        // 方法1
//        DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> ds = streamEnv.addSource(
//                getKafkaSource("test", "localhost:9092", "latest"))
//        Table a = tableEnv.fromDataStream(
//                ds
//                , "topic,offset,msg");

        // 方法2
        Kafka kafkaConnector =
                TableSourceConnectorManager.kafkaConnector("localhost:9092", "test", "test", "latest");
        Json jsonFormat = ConnectorFormatDescriptorUtils.kafkaConnJsonFormat(kafkaConnector);
        tableEnv
                .connect(kafkaConnector)
                .withFormat(jsonFormat)
                .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA())
                .inAppendMode()
                .createTemporaryTable("test");
        tableEnv.toRetractStream(tableEnv.sqlQuery("select * from test"), Row.class)
                .print();

        tableEnv.execute("");

    }


    @Test
    public void testTableSink() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg").addColumns("offset as ll");
         // sink1 : 转 stream后sink
         tableEnv.toAppendStream(a, Row.class).print();
        // 使用 connect的方式
        // sink3
        // TableSinkManager.connctKafkaSink(tableEnv, "test_sink_kafka");
        // a.insertInto("test_sink_kafka");
        // TableSinkManager.connectFileSystemSink(tableEnv, "test_sink_csv");
        // a.insertInto("test_sink_csv");


        // sink2 : 也是过期的，改用 connector方式 ，需要自己实现 TableSinkFactory .参考csv
        // TableSinkManager.registAppendStreamTableSink(tableEnv);
        // a.insertInto("test2");


        // sink4 : register 的方式已经过期，用conector的方式
//        String[] s = {"topic", "offset", "msg"};
//        TypeInformation[] ss = {Types.STRING, Types.LONG, Types.STRING};
//        TableSinkManager.registerJavaCsvTableSink(
//                tableEnv,
//                "test_sink_csv",
//                s,
//                ss,
//                "file:///Users/eminem/workspace/flink/flink-learn/checkpoint/data", // output path
//                "|", // optional: delimit files by '|'
//                1, // optional: write to a single file
//                FileSystem.WriteMode.OVERWRITE
//        );
        // a.insertInto("test_sink_csv");

        tableEnv.execute("");
    }

    /**
     * 自定义的sinkfactory
     * 百度SPI
     * 1： 需要再resources/META-INF.services/下创建一个接口名-(org.apache.flink.table.factories.TableFactory)的SPI文件（不是txt）
     * 2：在文件里面写上自己实现的类路径
     * 3：实现PrintlnAppendStreamFactory。
     */
    @Test
    public void testcustomSinkFactory() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg").renameColumns("offset as ll"); // offset是关键字

        tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomSinkTbl("printlnSinkTbl"));

        tableEnv.insertInto("printlnSinkTbl", a.select("topic,ll,msg"));

        tableEnv.execute("");

    }
}
