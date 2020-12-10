package com.flink.java.test;

import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetTimeMsg;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Collection;

public class FlinkCoreWindowTest extends FlinkJavaStreamTableTestBase {

    /**
     * window 有两种 Windowfunction ：
     * AggregateFunction(一条一条聚合)
     * ProcessindowFunction 触发的时候一次性聚合， Iterator给你数据
     */
    @Test
    public void testWindow() throws Exception {
        KafkaSourceManager.getKafkaDataStream(streamEnv,
                "test",
                "localhost:9092",
                "latest", new TopicOffsetTimeStampMsgDeserialize())
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<KafkaTopicOffsetTimeMsg>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(KafkaTopicOffsetTimeMsg element) {
                                return element.ts();
                            }
                        })
                .returns(KafkaTopicOffsetTimeMsg.class)
                .map((MapFunction<KafkaTopicOffsetTimeMsg, Tuple2<String, Long>>) value -> new Tuple2<>(value.msg(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(0)
                .timeWindow(Time.seconds(2)) // 统计5s一个窗口
                // .trigger(EventTimeTrigger.create()) // 以eventtime 时间触发窗口，当wartermark 》 window endtime 触发
                // .evictor(TimeEvictor.of(Time.seconds(2))) // 只保留 窗口内最近2s的数据做计算
                .process(new ProcessWindowFunction<Tuple2<String, Long>,
                        Tuple3<TimeWindow, String, Long>,
                        Tuple1<String>,
                        TimeWindow>() {
                    @Override
                    public void process(Tuple1<String> s,
                                        Context context,
                                        Iterable<Tuple2<String, Long>> elements,
                                        Collector<Tuple3<TimeWindow, String, Long>> out) throws Exception {
                        long count = 0;
                        for (Tuple2<String, Long> in : elements) {
                            count++;
                        }
                        out.collect(new Tuple3(context.window(), s.f0, count));
                    }
                })
//                 .sum(1)
                .setParallelism(4)
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
                        new BoundedOutOfOrdernessTimestampExtractor<KafkaTopicOffsetTimeMsg>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(KafkaTopicOffsetTimeMsg element) {
                                return element.ts();
                            }
                        })
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
