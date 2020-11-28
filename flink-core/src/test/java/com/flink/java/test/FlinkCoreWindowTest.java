package com.flink.java.test;

import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetTimeMsg;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class FlinkCoreWindowTest extends FlinkJavaStreamTableTestBase {

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
                        for (Tuple2<String, Long> in: elements) {
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
}
