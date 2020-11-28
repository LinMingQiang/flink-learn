package com.flink.java.function.process;

import com.flink.common.kafka.KafkaManager;
import org.apache.commons.lang3.time.DateFormatUtils;
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

    /**
     * 初始化
     * @param parameters
     * @throws Exception
     */
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

    /**
     * register time server 触发时执行
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        if (v2.value() != null) {
            v1.get().forEach(x -> {
                try {
                    out.collect("等待后Join ：" + x.msg() + " - " + v2.value().msg());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
            v1.get().forEach(x -> {
                if(x.ts() <= timestamp){
                    ctx.output(errV1, "过期未Join : " + x);
                }
            });
        }
        v1.clear();
    }

    /**
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement1(KafkaManager.KafkaTopicOffsetTimeMsg value, Context ctx, Collector<String> out) throws Exception {
        System.out.println(value+ "  v1 wtm : " + DateFormatUtils.format(ctx.timerService().currentWatermark(), "yyyy-mm-dd HH:mm:ss"));
        if (v2.value() == null) {
            v1.add(value);
            System.out.println("未Join到等待 : " + ctx.getCurrentKey());
            ctx.timerService().registerEventTimeTimer(value.ts()); // 当wartermark超过这个时间的时候就触发ontimer
        } else {
            out.collect("join到 ：" + value.msg() + " - " + v2.value().msg());
            v1.get().forEach(x -> {
                try {
                    out.collect("历史的也跟着一起触发Join ：" + x.msg() + " - " + v2.value().msg());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            v1.clear();
        }
    }

    /**
     * 维表数据做记录
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement2(KafkaManager.KafkaTopicOffsetTimeMsg value, Context ctx, Collector<String> out) throws Exception {
        System.out.println(value +  "  v2 wtm : " + DateFormatUtils.format(ctx.timerService().currentWatermark(), "yyyy-mm-dd HH:mm:ss"));
        v2.update(value);
        v1.get().forEach(x -> {
            try {
                out.collect("历史的也跟着一起触发Join ：" + x.msg() + " - " + v2.value().msg());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        v1.clear();
    }
}
