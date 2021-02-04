package com.flink.java.test;

import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetTimeMsg;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.time.Duration;
import java.util.Collection;

public class FlinkCoreWindowTest extends FlinkJavaStreamTableTestBase {

    /**
     * window 有两种 Windowfunction ：
     * AggregateFunction(一条一条聚合)
     * ProcessindowFunction 触发的时候一次性聚合， Iterator给你数据
     */
    @Test
    public void testWindow() throws Exception {
        // {"ts":10,"msg":"c"}
        initJsonSource(true);
        d1
                .map((MapFunction<KafkaTopicOffsetTimeMsg, Tuple2<String, Long>>) value -> new Tuple2<>(value.msg(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy((KeySelector<Tuple2<String, Long>, String>) o -> o.f0)
                // 统计5s一个窗口，有个offset参数，用来调整时间起点，正常是00-05这样，可以调成 01-06
                .window(TumblingEventTimeWindows.of(Time.seconds(100)))
                // 固定时间触发, 每10s触发一次(系统时间) .如果没有设置，则是根据eventtime > window end time 来决定触发
                 .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(2)))
                // .trigger(EventTimeTrigger.create()) // 以eventtime 时间触发窗口，当wartermark 》 window endtime 触发
                 .evictor(TimeEvictor.of(Time.seconds(0), true)) // 只保留 窗口内最近2s的数据做计算
                // 在process才产生 Transformation，上面的定义存在 WindowOperatorBuilder 里面
                // 这个可以拿到窗口的所有数据，效率低，可以使用AggregateFunction或者reduceFunction。
                .process(new ProcessWindowFunction<Tuple2<String, Long>,
                        Tuple3<TimeWindow, String, Long>,
                        String,
                        TimeWindow>() {
                    // 如果定义了trigger，就每次都会调用这个方法，就会多次计算
                    // 最好的方法就是定期清理窗口的数据，不然每次触发都是拿窗口的全部数据做计算
                    @Override
                    public void process(String s,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<Tuple3<TimeWindow, String, Long>> out) throws Exception {
                        System.out.println(">>>>>");
                        long count = 0;
                        for (Tuple2<String, Long> in : elements) {
                            count++;
                        }
                        out.collect(new Tuple3(context.window(), s, count));
                    }
                })
//                .sum(1)
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }


    @Test
    public void testWindowAll() throws Exception {
        KafkaSourceManager.getKafkaDataStream(streamEnv,
                "test",
                "localhost:9092",
                "latest", new TopicOffsetTimeStampMsgDeserialize())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<KafkaManager.KafkaTopicOffsetTimeMsg>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(((element, recordTimestamp) -> element.ts())))
                .returns(KafkaTopicOffsetTimeMsg.class)
                .map((MapFunction<KafkaTopicOffsetTimeMsg, Tuple2<String, Long>>) value -> new Tuple2<>(value.msg(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
                        int c = 0;
                        for (Tuple2<String, Long> element : elements) {
                            c++;
                        }
                        out.collect(context.window() + " : " + c);
                    }
                })
                // .setParallelism(4) // 不可设置，并行度必须为1
                .print();
        streamEnv.execute();
    }
}
