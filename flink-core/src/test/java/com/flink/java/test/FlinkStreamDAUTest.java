package com.flink.java.test;

import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetTimeMsg;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
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

public class FlinkStreamDAUTest extends FlinkJavaStreamTableTestBase {

    /**
     * DAU ： 一天的窗口，每5s输出一次。
     * 不能用滑动窗口， 用翻滚窗口+trigger （翻滚窗口肯定是一天的 0-24，不会有滑动的问题）
     */
    @Test
    public void testWindowDAU() throws Exception {
        // {"ts":5,"msg":"hello"} {"ts":10,"msg":"hello2"} {"ts":15,"msg":"hello"} {"ts":20,"msg":"hello"}
        // {"ts":25,"msg":"hello3"} {"ts":99,"msg":"hello4"} {"ts":101,"msg":"hello"} {"ts":140,"msg":"hello"}
        initJsonCleanSource();
        cd1
                .keyBy((KeySelector<KafkaTopicOffsetTimeMsg, String>) value -> value.topic()) // 按key分配
                .window(TumblingEventTimeWindows.of(Time.seconds(100L))) // 统计100s一个窗口
                // 因为用的是系统时间，所以一个窗口会被多次触发，除非wtm超过了这个窗口的endtime，否则窗口一直保留.用processtime可以多次触发
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(3))) // 固定时间触发, 每5s触发一次(系统时间)
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                // 如果不加这个， Iterable<KafkaTopicOffsetTimeMsg> elements 的数据就一直累积。
                // 每次计算完都清除 窗口数据。(只是清理原始数据process的数据会保留，也就是说，每次计算的时候，都是计算都是拿着5s中的数据进入process计算)
                .process(new ProcessWindowFunction<KafkaTopicOffsetTimeMsg,
                        Tuple2<TimeWindow, Tuple3<String, Long, Long>>,
                        String,
                        TimeWindow>() {
                    ValueStateDescriptor<Long> pvDec = null;
                    MapStateDescriptor<String, String> uidDec = null;
                    ValueStateDescriptor<Long> uvDec = null;

                    @Override
                    public void open(Configuration parameters) {
                        uidDec = new MapStateDescriptor("uidState", Types.STRING, Types.STRING);
                        pvDec = new ValueStateDescriptor<Long>("pvCount", Types.LONG, 0L);
                        uvDec = new ValueStateDescriptor<Long>("uvCount", Types.LONG, 0L);
                    }
                    // 不同的窗口也会进来，所以必须用 context.windowState() ,这个是窗口自己的state。如果在外面定义，那就是operate state，所有公用的
                    @Override
                    public void process(String groupKey,
                                        Context context,
                                        Iterable<KafkaTopicOffsetTimeMsg> elements,
                                        Collector<Tuple2<TimeWindow, Tuple3<String, Long, Long>>> out) throws Exception {
                        ValueState<Long> pvState = context.windowState().getState(pvDec);
                        ValueState<Long> uvState = context.windowState().getState(uvDec);
                        MapState<String, String> uidsState = context.windowState().getMapState(uidDec);
                        Long pv = pvState.value();
                        Long uv = uvState.value();
                        for (KafkaTopicOffsetTimeMsg element : elements) {
                            if (!uidsState.contains(element.msg())) {
                                uidsState.put(element.msg(), null);
                                uv++;
                            }
                            pv++;
                        }
                        pvState.update(pv);
                        uvState.update(uv);
                        out.collect(new Tuple2(context.window(), new Tuple3(groupKey, pv, uvState.value())));
                    }

                    /**
                     * 当窗口被清理的时候调用对应窗口的clear
                     * @param context
                     * @throws Exception
                     */
                    @Override
                    public void clear(Context context) throws Exception {
                        System.out.println("clear : " + context.window());
                        ValueState<Long> pvState = context.windowState().getState(pvDec);
                        ValueState<Long> uvState = context.windowState().getState(uvDec);
                        MapState<String, String> uidsState = context.windowState().getMapState(uidDec);
                        pvState.clear();
                        uvState.clear();
                        uidsState.clear();
                        super.clear(context);
                    }
                })
                .setParallelism(4)
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}
