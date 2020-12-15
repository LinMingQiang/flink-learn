package com.flink.java.test;

import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetTimeMsg;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.table.api.Expressions.*;
import static org.apache.flink.table.api.Expressions.$;

public class FlinkStreamBusinessTest extends FlinkJavaStreamTableTestBase {

    /**
     * DAU ： 一天的窗口，每5s输出一次。
     */
    @Test
    public void testWindowDAU() throws Exception {
        // {"ts":5,"msg":"hello"} {"ts":10,"msg":"hello"} {"ts":15,"msg":"hello"} {"ts":20,"msg":"hello2"}
        // {"ts":25,"msg":"hello3"} {"ts":30,"msg":"hello4"} {"ts":35,"msg":"hello"} {"ts":40,"msg":"hello"}
        initJsonCleanSource();
        cd1
                .filter((FilterFunction<KafkaTopicOffsetTimeMsg>) value -> value.ts() >= 1600000000000L) // 过滤昨天的数据
                .keyBy((KeySelector<KafkaTopicOffsetTimeMsg, String>) value -> value.topic()) // 按key分配
                .window(TumblingProcessingTimeWindows.of(Time.seconds(100L))) // 统计5s一个窗口
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5))) // 固定时间触发, 每10s触发一次(系统时间)
                .evictor(TimeEvictor.of(Time.seconds(0), true)) // 每次计算完都清除 窗口数据。
                .process(new ProcessWindowFunction<KafkaTopicOffsetTimeMsg,
                        Tuple2<TimeWindow, Tuple3<String, Long, Long>>,
                        String,
                        TimeWindow>() {
                    MapState<String, String> UidState = null;
                    ValueState<Long> pvCount = null;
                    ValueState<Long> uvCount = null;

                    @Override
                    public void open(Configuration parameters) {
                        UidState = getRuntimeContext().getMapState(new MapStateDescriptor<String, String>("uids", Types.STRING, Types.STRING));
                        pvCount = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pvCount", Types.LONG, 0L));
                        uvCount = getRuntimeContext().getState(new ValueStateDescriptor<Long>("uvCount", Types.LONG, 0L));
                    }

                    // 保存每个key下面的所有uid。
                    @Override
                    public void process(String groupKey,
                                        Context context,
                                        Iterable<KafkaTopicOffsetTimeMsg> elements,
                                        Collector<Tuple2<TimeWindow, Tuple3<String, Long, Long>>> out) throws Exception {
                        Long pv = pvCount.value();
                        Long uv = uvCount.value();
                        for (KafkaTopicOffsetTimeMsg element : elements) {
                            if (!UidState.contains(element.msg())) {
                                UidState.put(element.msg(), null);
                                uv++;
                            }
                            pv++;
                        }
                        pvCount.update(pv);
                        uvCount.update(uv);
                        out.collect(new Tuple2(context.window(), new Tuple3(groupKey, pv, uvCount.value())));
                    }
                })
                .setParallelism(4)
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}
