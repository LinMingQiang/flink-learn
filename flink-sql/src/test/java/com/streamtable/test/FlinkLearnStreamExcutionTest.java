package com.streamtable.test;

import com.flink.common.java.connect.PrintlnConnect;
import com.flink.common.java.pojo.KafkaTopicOffsetMsgPoJo;
import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.common.java.tablesink.HbaseRetractStreamTableSink;
import com.flink.common.kafka.KafkaManager;
import com.flink.common.manager.SchemaManager;
import com.flink.common.manager.TableSourceConnectorManager;
import com.flink.java.function.common.util.AbstractHbaseQueryFunction;
import com.flink.java.function.process.HbaseQueryProcessFunction;
import com.flink.learn.sql.common.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FlinkLearnStreamExcutionTest extends FlinkJavaStreamTableTestBase {
    /**
     * table 转stream
     *
     * @throws Exception
     */
    @Test
    public void testTableToStream() throws Exception {
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg");
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
        Json jsonFormat = ConnectorFormatDescriptorUtils.kafkaConnJsonFormat();
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
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg").renameColumns("offset as ll");
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
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg");
        tableEnv.createTemporaryView("test", a);


//        // 可以转stream之后再转换。pojo可以直接对应上Row
//        SingleOutputStreamOperator<Tuple2<String, Row>> ds = tableEnv.toAppendStream(a, KafkaTopicOffsetMsgPoJo.class)
//                .map(new MapFunction<KafkaTopicOffsetMsgPoJo, Tuple2<String, Row>>() {
//                    @Override
//                    public Tuple2<String, Row> map(KafkaTopicOffsetMsgPoJo value) throws Exception {
//                        return new Tuple2<>(value.topic, Row.of(value.toString()));
//                    }
//                });
        // tableEnv.createTemporaryView("test", ds);
        // 方法1
        tableEnv.registerTableSink("hbasesink",
                new HbaseRetractStreamTableSink(new String[]{"topic", "c"},
                        new DataType[]{DataTypes.STRING(), DataTypes.BIGINT()
                        }));

        // 方法2
//        tableEnv.sqlUpdate(DDLSourceSQLManager.createCustomHbaseSinkTbl("hbasesink"));


        tableEnv.sqlQuery("select topic,count(1) as c from test  group by topic")
                .insertInto("hbasesink");
        tableEnv.execute("");

    }

    /**
     * join 维表，维表大
     * 批量查询hbase，
     *
     * @throws Exception
     */
    @Test
    public void testHbaseJoin() throws Exception {
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg");
        OutputTag<KafkaTopicOffsetMsgPoJo> queryFailed = new OutputTag<KafkaTopicOffsetMsgPoJo>("queryFailed") {
        };
        SingleOutputStreamOperator t = tableEnv
                .toAppendStream(a, KafkaTopicOffsetMsgPoJo.class)
                .keyBy((KeySelector<KafkaTopicOffsetMsgPoJo, String>) value -> value.msg)
                .process(new HbaseQueryProcessFunction<KafkaTopicOffsetMsgPoJo, WordCountPoJo>(
                        new AbstractHbaseQueryFunction<KafkaTopicOffsetMsgPoJo, WordCountPoJo>() {
                            @Override
                            public String getRowkey(KafkaTopicOffsetMsgPoJo input) {
                                return input.msg;
                            }

                            @Override
                            public void transResult(Tuple2<Result, KafkaTopicOffsetMsgPoJo> res, List<WordCountPoJo> result) {
                                if (res.f0 == null)
                                    result.add(new WordCountPoJo("joinfail", 1L));
                                else {
                                    result.add(new WordCountPoJo(res.f1.msg, 1L));
                                }
                            }
                        },
                        null,
                        100,
                        TypeInformation.of(KafkaTopicOffsetMsgPoJo.class),
                        queryFailed))
                .returns(TypeInformation.of(WordCountPoJo.class)).uid("uid").name("name");

        t.getSideOutput(queryFailed)
                .map(x -> "cant find : " + x.toString())
                .print();

        tableEnv.createTemporaryView("wcstream", t);

        // retract
        tableEnv.toRetractStream(
                tableEnv.sqlQuery("select word,sum(num) num from wcstream group by word"),
                TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                })) // Row.class
                .print();
        // upsert
//        tableEnv.connect(new PrintlnConnect("printsink_upsert", 1, true))
//                .inRetractMode()
//                .withFormat(ConnectorFormatDescriptorUtils.kafkaConnJsonFormat())
//                .withSchema(SchemaManager.WORD_COUNT_SCHEMA())
//                .createTemporaryTable("printsink_upsert");
//        tableEnv.sqlUpdate("insert into printsink_upsert select word,sum(num) num from wcstream group by word");


        tableEnv.execute("");
    }


    /**
     * 时间的几种定义
     * 1: pt.proctime ： 默认就有 proctime属性，pt为它的命名
     * 2: user_action_time AS PROCTIME()
     */
    @Test
    public void testTimeAttributes() throws Exception {
        // {"id":"id2","name":"name","age":1}
        // 方法1 ： Processtime
//        Table a = getStreamTable(
//                getKafkaDataStream("test", "localhost:9092", "latest"),
//                "topic,offset,msg,pt.proctime")
//                .renameColumns("offset as ll"); // offset是关键字
//        tableEnv.createTemporaryView("test", a);
//        tableEnv.toAppendStream(a, Row.class).print();
//
//
//        // 方法2 ： Processtime
//        tableEnv.sqlUpdate(DDLSourceSQLManager.createStreamFromKafkaProcessTime(
//                "localhost:9092",
//                "localhost:2181",
//                "test", "test2", "test2"));
//        tableEnv.toAppendStream(tableEnv.from("test2"), Row.class).print();


        // {"id":"id2","name":"name","age":1,"etime":1596423467685}
// eventtime
//        Table a = getStreamTable(
//                getKafkaDataStreamWithEventTime("test", "localhost:9092", "latest")
//                        .assignTimestampsAndWatermarks(
//                                new BoundedOutOfOrdernessTimestampExtractor<KafkaManager.KafkaTopicOffsetMsgEventtime>(Time.seconds(10)) {
//                                    @Override
//                                    public long extractTimestamp(KafkaManager.KafkaTopicOffsetMsgEventtime element) {
//                                        return element.etime();
//                                    }
//                                }),
//                "topic,offset,msg,etime.rowtime");// offset是关键字
//        tableEnv.createTemporaryView("test", a);
//        tableEnv.toAppendStream(a, Row.class).print();

        // eventtime
        tableEnv.sqlUpdate(DDLSourceSQLManager.createStreamFromKafkaEventTime(
                "localhost:9092",
                "localhost:2181",
                "test", "test2", "test2"));

        tableEnv.toAppendStream(tableEnv.from("test2"), Row.class).print();


        tableEnv.execute("");

    }
}
