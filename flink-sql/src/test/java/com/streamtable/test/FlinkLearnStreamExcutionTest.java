package com.streamtable.test;

import com.flink.commom.scala.streamsink.TableSinkManager;
import com.flink.common.java.pojo.KafkaTopicOffsetMsgPoJo;
import com.flink.common.java.pojo.TestRowPoJo;
import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.common.java.tablesink.HbaseRetractStreamTableSink;
import com.flink.common.manager.SchemaManager;
import com.flink.common.manager.TableSourceConnectorManager;
import com.flink.function.common.AbstractHbaseQueryFunction;
import com.flink.function.process.HbaseQueryProcessFunction;
import com.flink.learn.sql.func.TimestampYearHour;
import com.flink.learn.sql.func.TimestampYearHourTableFunc;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import com.flink.sql.common.format.ConnectorFormatDescriptorUtils;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import com.ddlsql.DDLSourceSQLManager;

import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkLearnStreamExcutionTest extends FlinkJavaStreamTableTestBase {


    /**
     * 不推荐使用，建议使用ddl方式create table
     */
//    @Test
//    public void testCreateTable() throws Exception {
//        // {"id":"id2","name":"name2","age":1}
//        Kafka kafkaConnector =
//                TableSourceConnectorManager.kafkaConnector("localhost:9092", "test", "test", "latest");
//        Json jsonFormat = ConnectorFormatDescriptorUtils.kafkaConnJsonFormat();
//        tableEnv.executeSql(DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl("printlnSink_retract"));
//        tableEnv
//                .connect(kafkaConnector)
//                .withFormat(jsonFormat)
//                .withSchema(SchemaManager.ID_NAME_AGE_SCHEMA())
//                .inAppendMode()
//                .createTemporaryTable("test");
//        tableEnv.executeSql("insert into printlnSink_retract select id as topic,name as msg,count(*) as cnt from test group by id,name");
//        //      tableEnv.toRetractStream(a, Row.class).print();
//        streamEnv.execute("aa");
//    }


    /**
     * stream 转 table 转 stream
     * KafkaTopicOffsetTimeMsg(topic: String, offset: Long,  ts: Long, date: String, msg: String)
     * 注意 一旦 Table 被转化为 DataStream，必须使用 StreamExecutionEnvironment 的 execute 方法执行该 DataStream 作业。
     *
     * @throws Exception
     */
    @Test
    public void testTableToStream() throws Exception {
        // {"ts":100,"msg":"hello"}
        initJsonCleanSource();
        Table a = getStreamTable(cd1, "topic,offset,ts,date,msg");
        tableEnv.createTemporaryView("test", a);

        tableEnv.executeSql(DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl("printlnSink_retract"));
        // 方式1
        tableEnv.executeSql("insert into printlnSink_retract select topic,msg,count(*) as ll from test group by topic,msg");

        // 方式2
//      Table b = tableEnv.sqlQuery("select topic,msg,count(*) as ll from test group by topic,msg");
//      b.executeInsert("printlnSink_retract");

        // 方式3
//        tableEnv.toRetractStream(
//                tableEnv.sqlQuery("select topic,msg,count(*) as ll from test group by topic,msg"),
//                Row.class)
//                .print();

        streamEnv.execute("jobname");
    }
    /**
     * stream 转 table ， table 转stream ，stream再转table
     * @throws Exception
     */
    @Test
    public void testStreamToTable() throws Exception {
        // {"ts":100,"msg":"hello"}
        initJsonCleanSource();
        Table a = getStreamTable(cd1, "topic,offset,ts,date,msg");
        tableEnv.createTemporaryView("test", a);

        // table -
        DataStream stream = tableEnv.toRetractStream(
                tableEnv.sqlQuery("select topic,msg,count(*) as ll from test group by topic,msg"),
                Row.class)
                .filter(x -> x.f0)
                .map(x -> new Tuple3<>(x.f1.getField(0).toString(), x.f1.getField(1).toString(), Long.valueOf(x.f1.getField(2).toString())))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG));
        // stream - table
        tableEnv.createTemporaryView("tmptale", tableEnv.fromDataStream(stream, "topic,msg,ll"));
        // table - stream
        tableEnv.toRetractStream(tableEnv.from("tmptale"), Row.class).print();
        streamEnv.execute("testStreamToTable");
    }

    /**
     * inner join的状态不会清楚，会一直保持下去
     * @throws Exception
     */
    @Test
    public void testInnerJoin() throws Exception {
        // {"ts":1000,"msg":"hello"}  {"ts":500,"msg":"hello"}
        initJsonCleanSource();
        Table left = getStreamTable(cd1, "topic,offset,ts,date,msg");
        Table right = getStreamTable(cd2, $("topic").as("topic1") ,$("msg").as("msg2"));

        Table result = left.join(right)
                .where($("msg").isEqual($("msg2")))
                .select($("*"));

        tableEnv.toRetractStream(
                result,
                Row.class)
                .print();
        streamEnv.execute("jobname");
    }

    @Test
    public void testSelect() throws Exception {
        // {"ts":1000,"msg":"hello2"}  {"ts":500,"msg":"hello"}
        initJsonCleanSource();
        Table orders = getStreamTable(cd1, "topic,offset,ts,date,msg");
        Table revenue = orders
                .filter($("msg").isNotEqual("hello"))
                .groupBy($("msg"), $("topic"))
                .select($("topic"), $("msg"), $("offset").sum().as("ll"));
        tableEnv.toRetractStream(
                revenue,
                Row.class)
                .print();
        streamEnv.execute("testSelect");
    }





    @Test
    public void testStreamTableSink() throws Exception {
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg")
                .renameColumns("offset as ll");
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
        // 方法1 。已经不推荐使用了，推荐的是ddl的方式
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
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg,pt.proctime")
                .renameColumns("offset as ll"); // offset是关键字
        tableEnv.createTemporaryView("test", a);
        tableEnv.toAppendStream(a, Row.class).print();
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


    @Test
    public void testRowPoJo() throws Exception {
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg")
                .renameColumns("offset as offsets");
        tableEnv.createTemporaryView("test", a);
        tableEnv.createTemporarySystemFunction("timestampYearHour", TimestampYearHour.class);
        // oay_month_hour必须是放在字段排序后的位置，否则cast错误
//        Table b = tableEnv.sqlQuery("select msg,offsets,timestampYearHour(100000000) as oay_month_hour,topic from test");
//        tableEnv.toRetractStream(b,
//                TestRowPoJo.class)
//                .print();
        Table b = tableEnv.sqlQuery("select msg,timestampYearHour(100000000) as oay_month_hour,offsets,topic from test");
        tableEnv.toRetractStream(b,
                TestRowPoJo.class)
                .print();
        streamEnv.execute("");
    }


    @Test
    public void testTableFunction() throws Exception {
        Table a = getStreamTable(
                getKafkaDataStream("test", "localhost:9092", "latest"),
                "topic,offset,msg")
                .renameColumns("offset as offsets");
        tableEnv.createTemporaryView("test", a);
        tableEnv.createTemporarySystemFunction("TimestampYearHourTableFunc", TimestampYearHourTableFunc.class);
        Table b = tableEnv.sqlQuery("select tttable.*,tmpTable.*,tmpTable2.* from test as tttable," +
                " LATERAL TABLE(TimestampYearHourTableFunc(100000000)) AS tmpTable(d, m, h)," +
                "LATERAL TABLE(TimestampYearHourTableFunc(100000000)) AS tmpTable2(d1, m1, h1)");
        b.printSchema();
        tableEnv.toRetractStream(b,
                Row.class)
                .print();
        streamEnv.execute("");
    }
}
