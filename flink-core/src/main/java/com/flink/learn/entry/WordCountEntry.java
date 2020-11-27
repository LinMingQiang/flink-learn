package com.flink.learn.entry;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.TopicOffsetMsgDeserialize;
import com.flink.common.java.core.FlinkEvnBuilder;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

public class WordCountEntry {
    public static StreamExecutionEnvironment streamEnv = null ;
    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
         streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                10000L);
//        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
//                streamEnv,
//                Time.minutes(1),
//                Time.minutes(6));
        runWordCount();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }


    /**
     * wordcount
     */
    public static void runWordCount(){
        DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> s1 =
                KafkaSourceManager.getKafkaDataStream(streamEnv, "test", "localhost:9092", "latest", new TopicOffsetMsgDeserialize());

        s1
                .flatMap((FlatMapFunction<KafkaManager.KafkaTopicOffsetMsg, String>) (value, out) -> {
                    for (String s : value.msg().split(",", -1)) {
                        out.collect(s);
                    }
                })
                .returns(Types.STRING)
                .map(x -> new Tuple2(x, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(1)
                .print();
    }
}
