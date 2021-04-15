package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkJiraBugTest extends FlinkJavaStreamTableTestBase {
//
//
//    /**
//     * 如果
//     * @throws Exception
//     */
//    @Test
//    public void udftest() throws Exception {
//        tableEnv.createTemporaryView("test" ,tableEnv.fromDataStream(streamEnv.fromElements("a", "b", "c", "d", "e"), $("msg")));
//        tableEnv.registerFunction("sample", new SampleFunction());
//        Table tm = tableEnv.sqlQuery("select msg, sample() as c from test");
//        tableEnv.createTemporaryView("tmp", tm);
////        System.out.println(r.explain());
//
//        tableEnv.toAppendStream(tm, Row.class).print();
//
//        tableEnv.toAppendStream(tableEnv.sqlQuery("select * from tmp where c > 5"), Row.class).print();
////        System.out.println(streamEnv.getExecutionPlan());
//        streamEnv.execute();
//    }
//
//
//    @Test
//    public void iftest2() throws Exception {
//        tableEnv.createTemporaryView("test" ,tableEnv.fromDataStream(streamEnv.fromElements("a", "", ""), $("msg")));
//        Table tm = tableEnv.sqlQuery("" +
//                "SELECT msg,\n" +
//                "IF(msg = '' OR msg IS NULL, 'N', 'Y') as ss\n" +
//                "FROM test\n" +
//                "\n");
//        tableEnv.toAppendStream(tm, Row.class).print();
//        streamEnv.execute();
//    }
//    // UDF函数
//    public class SampleFunction extends ScalarFunction {
//        public int eval() {
//           int a = (int) (Math.random() * 10);
//            System.out.println("************************" + a );
//            return a;
//        }
//    }


    /**
     * 在流表转换中的时间字段的定义
     * 正常情况下，我们要定义rowtime和watermark （只有窗口中用到）。我们只能在ddl。或者在TableApi的时候定义
     * 但是如果我们有一个Table里面带有时间字段，但他不是rowtime，这个表我们是无法使用window操作的。
     * 例如我们有个table是经过多个table转换过来的。
     * 1: 将table转stream
     * 2：在stream中定义watermark
     * 3：stream转table，同时指定rowtime
     * 注意：只要对表或者stream做了groupby就会导致 wtm不生成导致窗口无法触发。
     * 注意： 简单的表转换是不会丢失时间信息的。只要不变换时间字段
     * 注意：修改并行度，或者Datastream里面不能做keyby或者groupby操作(不管用哪个字段)，否则窗口触发不了,调试会发现窗口里面拿wtm是空
     */
    @Test
    public void stream2TableWatermarkErr() throws Exception {
        // {"rowtime":"2021-01-20 01:00:00","msg":"hello"}
        // {"rowtime":"2021-01-20 01:01:40","msg":"hello"}
        String srcSql = "CREATE TABLE test (\n" +
                "    rowtime TIMESTAMP(3),\n" +
                "    msg VARCHAR,\n" +
                "    WATERMARK FOR rowtime AS rowtime - INTERVAL '10' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'test',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'properties.bootstrap.servers' = 'localhost:9092',\n" +
                "    'properties.group.id' = 'test',\n" +
                "    'format' = 'json',\n" +
                "    'json.ignore-parse-errors' = 'true'\n" +
                ")";
        tableEnv.executeSql(srcSql);
        // 这里的group by和下面的keyBy 都导致不触发
        tableEnv.createTemporaryView("test2",
                tableEnv.sqlQuery("select msg,rowtime from test group by msg,rowtime"));

        SingleOutputStreamOperator r =
                tableEnv
                        .toRetractStream(tableEnv.from("test2"), Row.class)
//                        .keyBy((KeySelector<Tuple2<Boolean, Row>, String>) value -> value.f1.getField(0).toString())
                        .map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String, Long>>() {
                            SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                            @Override
                            public Tuple2<String, Long> map(Tuple2<Boolean, Row> value) throws Exception {
                                String formatstr = value.f1.getField(1).toString();
                                if (formatstr.length() < 19) formatstr += ":00";
                                return new Tuple2<>(value.f1.getField(0).toString(),
                                    s.parse(formatstr).getTime()
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(((element, recordTimestamp) -> element.f1))
                )
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                ;


        tableEnv.createTemporaryView("test3", r, $("msg"), $("rowtime").rowtime());




        String sql = "select " +
                "msg," +
                "count(1) cnt" +
                " from test3 " +
                " group by TUMBLE(rowtime, INTERVAL '10' SECOND), msg " +
                "";

        Table tm = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(tm, Row.class).print();
        streamEnv.execute();

    }
}
