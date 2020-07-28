package com.flink.java.function.process;

import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;

public class StreamConnectCoProcessFunc extends KeyedCoProcessFunction<Object, KafkaManager.KafkaTopicOffsetTimeMsg, KafkaManager.KafkaTopicOffsetTimeMsg, String> {
    // 一对多的场景，v2作为维表
    ListState<KafkaManager.KafkaTopicOffsetTimeMsg> v1 = null;
    ValueState<KafkaManager.KafkaTopicOffsetTimeMsg> v2 = null;
    OutputTag<String> errV1 = null;

    public StreamConnectCoProcessFunc(OutputTag<String> errV1) {
        this.errV1 = errV1;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        v1 = getRuntimeContext().getListState(
                new ListStateDescriptor("test1",
                        TypeInformation.of(KafkaManager.KafkaTopicOffsetTimeMsg.class)));
        v2 = getRuntimeContext().getState(
                new ValueStateDescriptor("test2",
                        TypeInformation.of(KafkaManager.KafkaTopicOffsetTimeMsg.class)));
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (v2.value() != null) {
            v1.get().forEach(x -> {
                try {
                    out.collect(x.msg() + " : " + v2.value().msg());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
            v1.get().forEach(x -> {
                    ctx.output(errV1, x.toString());
            });
        }
        v1.clear();
    }

    @Override
    public void processElement1(KafkaManager.KafkaTopicOffsetTimeMsg value, Context ctx, Collector<String> out) throws Exception {
        if (v2.value() == null) {
            v1.add(value);
            System.out.println("c : " + ctx.getCurrentKey() + " : " + value);
            ctx.timerService().registerProcessingTimeTimer(value.ts() + 10000L); // 10s钟
        } else {
            out.collect(value.msg() + " : " + v2.value().msg());
            v1.get().forEach(x -> {
                try {
                    out.collect(x.msg() + " : " + v2.value().msg());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            v1.clear();
        }
    }

    @Override
    public void processElement2(KafkaManager.KafkaTopicOffsetTimeMsg value, Context ctx, Collector<String> out) throws Exception {
        v2.update(value);
    }
}
