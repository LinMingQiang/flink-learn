package com.streamtable.test;

import com.pojo.KafkaTopicOffsetMsgPoJo;
import com.pojo.TestRowPoJo;
import com.pojo.WordCountPoJo;
import com.func.processfunc.AbstractHbaseQueryFunction;
import com.func.processfunc.HbaseQueryProcessFunction;
import com.flink.learn.sql.func.StrSplitTableFunction;
import com.flink.learn.sql.func.StrSplitToMultipleRowTableFunction;
import com.flink.learn.sql.func.TimestampYearHour;
import com.flink.learn.sql.func.TimestampYearHourTableFunc;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import com.ddlsql.DDLSourceSQLManager;

import java.util.List;

import static org.apache.flink.table.api.Expressions.*;

public class FlinkStreamTableApiTest extends FlinkJavaStreamTableTestBase {


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
     * append的只支持非 group类的sql输出
     *
     * @throws Exception
     */
    @Test
    public void testTableApiWordCount() throws Exception {
        tableEnv.createTemporaryView("test", kafkaDataTable);
        // 方式1
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        tableEnv.executeSql("insert into printlnRetractSink select msg,count(*) as cnt from test group by msg")
                .print();// 正常在main里面不需要print，test需要print才能执行

//        tableEnv
//                .toRetractStream(
//                        tableEnv.sqlQuery("select msg,count(*) as cnt from test group by msg"),
//                        Row.class)
//                .filter(x -> x.f0)
//                .map(x -> new Tuple2<>(x.f1.getField(0).toString(), Long.valueOf(x.f1.getField(1).toString())))
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .print();
//
//         streamEnv.execute(); // table转stream之后需要这个
    }

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
        tableEnv.createTemporaryView("test", kafkaDataTable);
        tableEnv.executeSql(DDLSourceSQLManager.createCustomPrintlnRetractSinkTbl("printlnSink_retract"));

        // 方式1
        //  tableEnv.executeSql("insert into printlnSink_retract select topic,msg,count(*) as ll from test group by topic,msg");
        // 方式2
        Table b = tableEnv.sqlQuery("select topic,msg,count(*) as ll from test group by topic,msg");
        TableResult re = b.executeInsert("printlnSink_retract");
        re.print();
        // 方式3
//        tableEnv.toRetractStream(
//                tableEnv.sqlQuery("select topic,msg,count(*) as ll from test group by topic,msg"),
//                Row.class)
//                .print();
//        streamEnv.execute("jobname");

    }

    /**
     * stream 转 table ， table 转stream ，stream再转table
     *
     * @throws Exception
     */
    @Test
    public void testStreamToTable() throws Exception {
        // {"ts":100,"msg":"hello"}
        tableEnv.createTemporaryView("test", kafkaDataTable);

        // table -
        DataStream stream = tableEnv.toRetractStream(
                tableEnv.sqlQuery("select topic,msg,count(*) as ll from test group by topic,msg"),
                Row.class)
                .filter(x -> x.f0)
                .map(x -> new Tuple3<>(x.f1.getField(0).toString(), x.f1.getField(1).toString(), Long.valueOf(x.f1.getField(2).toString())))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG));

        // table - stream
        printlnStringTable(tableEnv.fromDataStream(stream, "topic,msg,ll"));
        streamEnv.execute("");

    }

    /**
     * inner join的状态不会清楚，会一直保持下去
     * 注意： 如果两个表都有rowtime 时间字段，在select出来的时候只能选一个，
     * https://stackoverflow.com/questions/57181771/flink-rowtime-attributes-must-not-be-in-the-input-rows-of-a-regular-join
     *
     * @throws Exception
     */
    @Test
    public void testInnerJoin() throws Exception {
        // {"ts":1000,"msg":"hello"}  {"ts":500,"msg":"hello"}
        Table left = kafkaDataTable;
        Table right = getStreamTable(
                "test2", "localhost:9092","latest",
                $("topic").as("topic1"),
                $("msg").as("msg2"),
                $("ts").as("ts2"));
        Table result = left.join(right)
                .where($("msg").isEqual($("msg2")))
                .select($("*"));

        printlnStringTable(result);
        streamEnv.execute("");

    }

    /**
     * 对应ddl里面的  LATERAL TABLE
     *
     * @throws Exception
     */
    @Test
    public void testJoinUDTF() throws Exception {
        // {"ts":1000,"msg":"hello,world"}  {"ts":500,"msg":"hello"}
        Table orders = kafkaDataTable;
        tableEnv.createTemporarySystemFunction("ts_to_DMH", new TimestampYearHourTableFunc());
        tableEnv.createTemporarySystemFunction("split", new StrSplitTableFunction(","));
        tableEnv.createTemporarySystemFunction("split_multiple_row", new StrSplitToMultipleRowTableFunction(","));
        Table result = orders
                .joinLateral(call("ts_to_DMH", $("ts"))) // 因为返回的是Row，所以不需要as
                .joinLateral(call("split", $("msg")).as("key", "value")) // 一行转多列,不as的话是 f0,f1
                .joinLateral(call("split_multiple_row", $("msg")).as("v")) // 一行转多行，用两次会1条变4条
                .select($("*"));
        // TableAggregateFunction 用于多行转多行
        // 以下两个效果等同
        // orders.joinLateral(call("split_multiple_row", $("msg")).as("v")) // 一行转多行
        // orders.flatMap(call("split_multiple_row", $("msg")).as("v"));
        result.printSchema();
        printlnStringTable(result);
        streamEnv.execute("");

    }


    /**
     * ##好像没问题了？？：不能设置 setIdleStateRetentionTime 清除，否则报 java.lang.NullPointerException
     * join.temporal.BaseTwoInputStreamOperatorWithStateRetention.registerProcessingCleanupTimer(BaseTwoInputStreamOperatorWithStateRetention.java:109)
     * rowtime需要设置wtm。同时在数据上，最终的wtm是一起决定的
     *
     * @throws Exception
     */
    @Test
    public void testJoinTemporalTable() throws Exception {
        // test2 : {"ts":10,"msg":"1"} {"ts":15,"msg":"1"} {"ts":20,"msg":"1"} {"ts":29,"msg":"1"} ： wtm = 19
        // test1: {"ts":11,"msg":"1"} {"ts":16,"msg":"1"} {"ts":24,"msg":"1"} wtm = 14
        // 输入24的时候触发 11 。 再输入 11以前的
        // 输入 {"ts":25,"msg":"1"} 更新了wtm。  输入的11以前的数据也触发了。。。
        // 最后输入 {"ts":34,"msg":"1"} 只触发了 16的。因为两个流的 wtm还是19，并不是24.
        // 输入 test2 : {"ts":34,"msg":"1"} ,触发24  此时的wtm = 24
        // 可以得到 11 拿的10的 16拿的15的 21拿的20的
        Table orders = getStreamTable(kafkaDataSource,
                $("topic"),
                $("offset"),
                $("date"),
                $("msg").as("o_currency"),
                $("ts").rowtime().as("o_rowtime"));

        Table ratesHistory = getStreamTable(getKafkaKeyStream("test2", "localhost:9092", "latest"),
                $("topic").as("topic2"),
                $("offset").as("offset2"),
                $("date").as("date2"),
                $("msg").as("r_currency"),
                $("ts").rowtime().as("r_rowtime"));// 提供一个汇率历史记录表静态数据集

//        printlnStringTable(orders);
//        printlnStringTable(ratesHistory);

        TemporalTableFunction rates = ratesHistory.createTemporalTableFunction(
                $("r_rowtime"),
                $("r_currency"));
        tableEnv.createTemporarySystemFunction("rates", rates);


        Table result = orders
                .joinLateral(
                        call("rates", $("o_rowtime")),
                        $("o_currency").isEqual($("r_currency")))
                .select($("o_currency"), $("offset"), $("o_rowtime"),
                        lit("<< - >>").as("hello "),
                        $("r_rowtime"),
                        $("r_currency"), $("offset2"));
        printlnStringTable(result);
        streamEnv.execute("");

    }


    /**
     * @throws Exception
     */
    @Test
    public void testSelect() throws Exception {
        // {"ts":10,"msg":"hello"}  {"ts":31,"msg":"hello"}
        Table orders = getStreamTable(kafkaDataSource, $("topic"),
                $("offset"),
                $("date"),
                $("msg"),
                $("ts").rowtime());

        Table result = orders
                .filter($("msg").isNotEqual("hello"))
                .groupBy($("msg"), $("topic"))
                .select($("topic"), $("msg"), $("offset").sum().as("ll"));

        printlnStringTable(result);

        streamEnv.execute("");

    }

    @Test
    public void testStreamTableSink() throws Exception {
        Table a = kafkaDataTable.renameColumns("offset as ll");
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
        tableEnv.createTemporaryView("test", kafkaDataTable);
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
//        tableEnv.createTemporarySystemFunction("hbasesink",
//                new HbaseRetractStreamTableSink(new String[]{"topic", "c"},
//                        new DataType[]{DataTypes.STRING(), DataTypes.BIGINT()
//                        }));
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
        OutputTag<KafkaTopicOffsetMsgPoJo> queryFailed = new OutputTag<KafkaTopicOffsetMsgPoJo>("queryFailed") {
        };
        SingleOutputStreamOperator t = tableEnv
                .toAppendStream(kafkaDataTable, KafkaTopicOffsetMsgPoJo.class)
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

    @Test
    public void testRowPoJo() throws Exception {
        Table a = kafkaDataTable
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
        // {"rowtime":"2021-01-20 01:00:00","msg":"hello"}
        tableEnv.createTemporaryView("test", kafkaDataTable);
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


    @Test
    public void testOverWindow() throws Exception {
        // {"ts":10,"msg":"hello"} {"ts":21,"msg":"hello"} {"ts":40,"msg":"hello"}
        // $("user"), $("product"), $("amount"), $("rowtime").rowtime()
        Table a = getStreamTable(kafkaDataSource,
                $("topic"),
                $("offset"),
                $("msg"),
                $("date"),
                $("ts").rowtime());
        tableEnv.createTemporaryView("test", a);
        String sql = "select sum(distinct `offset`) over w,max(`offset`) over w " +
                "from test " +
                "window w as ( " +
                "PARTITION BY topic " +
                "order by ts " +
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ";
        Table b = a
                .window(Over
                        .partitionBy($("topic"))
                        .orderBy($("ts"))
                        .preceding(UNBOUNDED_ROW)
                        .as("w"))
                .select(
                        $("topic"),
                        $("msg").count().over($("w")),
                        $("msg").max().over($("w")),
                        $("msg").min().over($("w"))
                );
        System.out.println(b.explain());
        ;
        tableEnv.toAppendStream(b, Row.class).print();
//        tableEnv.toAppendStream(tableEnv.sqlQuery(sql), Row.class).print();


        streamEnv.execute();


    }
}
