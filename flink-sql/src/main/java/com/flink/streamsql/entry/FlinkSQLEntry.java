package com.flink.streamsql.entry;

import com.core.FlinkEvnBuilder;
import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.learn.sql.common.DDLSourceSQLManager;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLEntry {
    public static StreamTableEnvironment tableEnv = null;

    /**
     * 测试问题：当使用group by ，window不触发
     *
     * @param args
     * @throws Exception
     */
    // {"rowtime":"2021-01-20 01:01:00","msg":"hello"}
    // {"rowtime":"2021-01-20 01:11:40","msg":"hello"}
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = null;
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
                streamEnv, Duration.ofHours(1L));

        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        tableEnv.executeSql(DDLSourceSQLManager.createDynamicPrintlnRetractSinkTbl("printlnRetractSink"));
        tableEnv.createTemporaryView("test3", tableEnv.sqlQuery("select msg,rowtime from test group by msg,rowtime"));
//        tableEnv.createTemporaryView("test3", tableEnv.sqlQuery("select CONCAT(msg , '-hai') as msg,rowtime from test where msg is not null"));


// 1: table转stream。同时指定 wtm的时间抽取
        SingleOutputStreamOperator r = tableEnv.toRetractStream(tableEnv.from("test3"), Row.class)
                .filter(x -> x.f0)
                .map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<String, Long>>() {
                    SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public Tuple2<String, Long> map(Tuple2<Boolean, Row> value) throws Exception {
                        String formatstr = value.f1.getField(1).toString().replaceFirst("T", " ");
                        if (formatstr.length() < 19) formatstr += ":00";
                        return new Tuple2<>(value.f1.getField(0).toString(),
                                s.parse(formatstr).getTime()
                        );
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(((element, recordTimestamp) -> element.f1))
                );

        r.print();
        tableEnv.createTemporaryView("test5",
                r,
                $("msg"),
                $("rowtime").rowtime());
        String sql5 = "select " +
                "msg," +
                "count(1) cnt" +
                " from test5 " +
                " group by TUMBLE(rowtime, INTERVAL '30' SECOND), msg " +
                "";
        tableEnv.executeSql("insert into printlnRetractSink " + sql5);
//        streamEnv.execute();
    }
}
