package com.streamddl.test;

import com.ddlsql.DDLSourceSQLManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.Duration;

public class FlinkCDCTest extends FlinkJavaStreamTableTestBase {



    // INSERT INTO `order_info` VALUES (11, 'iwKjQD', '13320383859', 88.00, '1', 107, '1', 'cbXLKtNHWOcWzJVBWdAs', 'njjsnknHxsxhuCCeNDDi', '0937074290', '', '2020-06-18 15:56:34', NULL, NULL, NULL, NULL, NULL, 7);
    // UPDATE `order_info` SET consignee='iwKjQD3' where id = 1;
    @Test
    public void cdcTest() throws Exception {
        tableEnv.executeSql(DDLSourceSQLManager.cdcCreateTableSQL("order_info"));
        tableEnv
                .toRetractStream(tableEnv.from("order_info"), Row.class)
                .filter((FilterFunction<Tuple2<Boolean, Row>>) booleanRowTuple2 -> booleanRowTuple2.f0)
                .map((MapFunction<Tuple2<Boolean, Row>, Tuple2<String, String>>) booleanRowTuple2 ->
                        new Tuple2<>(booleanRowTuple2.f1.getField("id").toString(), booleanRowTuple2.f1.getField("id").toString())
                )
                .returns(Types.TUPLE(Types.STRING, Types.STRING))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(((element, recordTimestamp) -> System.currentTimeMillis())))
                .keyBy((KeySelector<Tuple2<String, String>, String>) row -> "1")
                .window(TumblingEventTimeWindows.of(Time.seconds(1000)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(50)))
                .reduce((ReduceFunction<Tuple2<String, String>>) (v1, v2) -> new Tuple2(v1.f0,
                                String.valueOf(Long.valueOf(v1.f1) + Long.valueOf(v2.f1)))
                        , new ProcessWindowFunction<Tuple2<String, String>,
                                Tuple2<TimeWindow, Long>,
                                String,
                                TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Tuple2<String, String>> elements, Collector<Tuple2<TimeWindow, Long>> out) throws Exception {
                                Long count = 0L;
                                for (Tuple2<String, String> element : elements) {
                                    count += Long.valueOf(element.f1);
                                }
                                out.collect(new Tuple2(context.window(), count));
                            }
                        })
                .print();

        tableEnv
                .toRetractStream(tableEnv.from("order_info"), Row.class).print();

        streamEnv.execute();
    }

    // 以cdc做为维表进行join。cdc第一次会把所有数据都load一遍
    // {"rowtime":"2020-06-18 20:00:30","msg":"1"}
    // UPDATE `order_info` SET consignee='1',create_time = '2020-06-18 20:00:15',img_url='15' where id = 1;
    // 需要两边的wtm才能触发
    @Test
    public void cdctemporalTableJoinTest() throws Exception {
        tableEnv.executeSql(DDLSourceSQLManager.cdcCreateTableSQL("order_info"));
        tableEnv.executeSql(
                DDLSourceSQLManager.createStreamFromKafka("localhost:9092",
                        "test",
                        "test",
                        "test",
                        "json"));
        // lookup table
//        tableEnv.executeSql(DDLSourceSQLManager.createHbaseLookupSource("hbaselookup"));
        tableEnv.toRetractStream(tableEnv.sqlQuery("" +
                "select o.msg,o.rowtime,r.img_url,r.create_time from test as o JOIN " +
                " order_info FOR SYSTEM_TIME AS OF o.rowtime r" +
                " on o.msg = r.consignee" +
                ""), Row.class)
                .print();
        streamEnv.execute();

    }
}
