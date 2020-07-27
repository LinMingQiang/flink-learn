package com.flink.java.function.process;

import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class StreamConnectCoProcessFunc extends KeyedCoProcessFunction<Object, KafkaManager.KafkaTopicOffsetMsg, KafkaManager.KafkaTopicOffsetMsg, String> {
    ValueState<KafkaManager.KafkaTopicOffsetMsg> v1 = null;
    ValueState<KafkaManager.KafkaTopicOffsetMsg> v2 = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        v1 = getRuntimeContext().getState(
                new ValueStateDescriptor<KafkaManager.KafkaTopicOffsetMsg>("test1",
                        TypeInformation.of(KafkaManager.KafkaTopicOffsetMsg.class)));
        v2 = getRuntimeContext().getState(
                new ValueStateDescriptor<KafkaManager.KafkaTopicOffsetMsg>("test2",
                        TypeInformation.of(KafkaManager.KafkaTopicOffsetMsg.class)));
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }

    @Override
    public void processElement1(KafkaManager.KafkaTopicOffsetMsg value, Context ctx, Collector<String> out) throws Exception {

    }

    @Override
    public void processElement2(KafkaManager.KafkaTopicOffsetMsg value, Context ctx, Collector<String> out) throws Exception {

    }
}
