package com.flink.learn.entry;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.core.FlinkSourceBuilder;
import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.kafka.KafkaManager;
import com.func.processfunc.StreamConnectCoProcessFunc;

import java.time.Duration;

public class StreamJoinStreamEntry {
    public static StreamExecutionEnvironment streamEnv = null;

    /**
     * 双流Join的场景： 1：维表流比较大无法使用其他方式 2：维表可能更新，可能延迟，主表也可能延迟，所以不能直接扔，需要有wtm来决定什么时候扔
     *
     * <p>还适用另一种情况，例如要做一个大的布隆过滤器，需要初始化布隆，可以往维表里面灌数据 方法： 1： 双流Connect 2： 将维表数据放state。 3：
     * 注册ontime，当wtm或者processtime超过时间就清理state，保证state只有最近的数据
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        FlinkSourceBuilder.init(
                FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL(),
                EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry",
                Duration.ofHours(2));
        streamEnv = FlinkSourceBuilder.streamEnv;
        SingleOutputStreamOperator<KafkaManager.KafkaMessge> source =
                FlinkSourceBuilder.getKafkaDataStreamSource("test", "localhost:9092", "latest");

        SingleOutputStreamOperator<KafkaManager.KafkaMessge> v2 =
                FlinkSourceBuilder.getKafkaDataStreamSource("test2", "localhost:9092", "latest");
        source.connect(v2)
                .keyBy(
                        KafkaManager.KafkaMessge::msg,
                        KafkaManager.KafkaMessge::msg) // join的条件,其实就是分组下
                .process(new StreamConnectCoProcessFunc(null))
                .print();

        streamEnv.execute();
    }
}
