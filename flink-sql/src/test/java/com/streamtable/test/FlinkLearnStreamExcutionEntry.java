package com.streamtable.test;

import com.flink.common.java.pojo.KafkaTopicOffsetMsgPoJo;
import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.common.java.tablesink.HbaseRetractStreamTableSink;
import com.flink.common.manager.SchemaManager;
import com.flink.common.manager.TableSourceConnectorManager;
import com.flink.java.function.rich.HbaseQueryFunction;
import com.flink.java.function.rich.HbaseQueryProcessFunction;
import com.flink.learn.sql.common.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkStreamTableTestBase;
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.apache.flink.api.*;

import java.util.ArrayList;
import java.util.List;

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
    public void testStreamTableSink() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg").renameColumns("offset as ll");
        // sink1 : 转 stream后sink
        // tableEnv.toAppendStream(a, Row.class).print();

        // String sql="insert into hbasesink select topic,count(1) as c from test  group by topic";
        // tableEnv.sqlUpdate(sql);

        // 使用 connect的方式
        // sink3
//         TableSinkManager.connctKafkaSink(tableEnv, "test_sink_kafka");
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
     * 将统计结果输出到hbase
     *
     * @throws Exception
     */
    @Test
    public void testcustomHbasesink() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("probe_box", "10.6.161.208:9092,10.6.161.209:9092,10.6.161.210:9092,10.6.161.211:9092,10.6.161.212:9092", "latest"))
                , "topic,offset,msg");
        // 可以转stream之后再转换。pojo可以直接对应上Row
        SingleOutputStreamOperator<Tuple2<String, Row>> ds = tableEnv.toAppendStream(a, KafkaTopicOffsetMsgPoJo.class)
                .map(new MapFunction<KafkaTopicOffsetMsgPoJo, Tuple2<String, Row>>() {
                    @Override
                    public Tuple2<String, Row> map(KafkaTopicOffsetMsgPoJo value) throws Exception {
                        return new Tuple2<>(value.topic, Row.of(value.toString()));
                    }
                });
        // tableEnv.createTemporaryView("test", ds);
        ds.print();
        //  new HbaseQueryRichFlatMapFunction()
        // 方法1
//        tableEnv.registerTableSink("hbasesink",
//                new HbaseRetractStreamTableSink(new String[]{"topic", "c"},
//                        new DataType[]{DataTypes.STRING(), DataTypes.BIGINT()
//                        }));

        //  new HbaseQueryFunction<Row, String>() {
        //                @Override
        //                public String transResult(List<Row> res) {
        //                    return res.toString();
        //                }
        //            };
        // 方法2
//        tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomHbaseSinkTbl("hbasesink"));
//        tableEnv.sqlQuery("select topic,count(1) as c from test  group by topic")
//                .insertInto("hbasesink");
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
        tableEnv.createTemporaryView("test", a);
        System.out.println(tableEnv.explain(tableEnv.sqlQuery("select count(1) from test")));

        tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomSinkTbl("printlnSinkTbl"));
        tableEnv.insertInto("printlnSinkTbl", a.select("topic, ll,msg"));
        tableEnv.execute("");
    }


    /**
     * 数据在hbase里面，但是
     * @throws Exception
     */
    @Test
    public void testHbaseJoin() throws Exception {
        Table a = tableEnv.fromDataStream(
                streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"))
                , "topic,offset,msg");
        // 可以转stream之后再转换。pojo可以直接对应上Row
        SingleOutputStreamOperator<Tuple2<String, Row>> ds = tableEnv.toAppendStream(a, KafkaTopicOffsetMsgPoJo.class)
                .map(new MapFunction<KafkaTopicOffsetMsgPoJo, Tuple2<String, Row>>() {
                    @Override
                    public Tuple2<String, Row> map(KafkaTopicOffsetMsgPoJo value) throws Exception {
                        return new Tuple2<>(value.topic, Row.of(value.toString()));
                    }
                });
        // tableEnv.createTemporaryView("test", ds);

        ds.keyBy(x -> x.f0)
        .process(new HbaseQueryProcessFunction(new HbaseQueryFunction<Tuple2<String, Row>, String>() {
            @Override
            public List<String> transResult(List<Tuple2<String, Row>> res) {
                List<String> r = new ArrayList<>();
                for (Tuple2<String, Row> row : res) {
                    r.add("Helloss : " + row.f1.toString());
                }
                return r;
            }
        }, TypeInformation.of(Row.class))).returns(TypeInformation.of(String.class))
        .print();
        tableEnv.execute("");
    }
}
