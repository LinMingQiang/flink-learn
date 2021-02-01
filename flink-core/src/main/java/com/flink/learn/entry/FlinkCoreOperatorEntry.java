package com.flink.learn.entry;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.TopicOffsetMsgDeserialize;
import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.core.FlinkEvnBuilder;
import com.flink.common.java.manager.KafkaSourceManager;
import com.flink.common.kafka.KafkaManager;
import com.flink.function.process.StreamConnectCoProcessFunc;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.time.Duration;

public class FlinkCoreOperatorEntry {
    public static StreamExecutionEnvironment streamEnv = null;

    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "WordCountEntry");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
//        StreamTableEnvironment tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
//                streamEnv,
//                Time.minutes(1),
//                Time.minutes(6));
        if(args.length > 0) {
            switch (args[0]) {
                case "runWordCount": runWordCount(); break;
                case "runStreamConnect":  runStreamConnect(); break;
                default:
                    System.out.println("未匹配 ：" + args[0]);break;
            }
        } else {
            runWordCount();
        }
        streamEnv.execute("FlinkCoreOperatorEntry"); //程序名
    }


    /**
     * wordcount
     */
    public static void runWordCount() {
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


    public static void runStreamConnect() throws Exception {
        // 10s过期
        OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {
        };
        SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> a =
                KafkaSourceManager.getKafkaDataStream(streamEnv,
                        "test",
                        "localhost:9092",
                        "latest", new TopicOffsetTimeStampMsgDeserialize())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<KafkaManager.KafkaTopicOffsetTimeMsg>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                        .withTimestampAssigner(((element, recordTimestamp) -> element.ts())))
                        .setParallelism(2);

        SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> b =
                KafkaSourceManager.getKafkaDataStream(streamEnv,
                        "test2",
                        "localhost:9092",
                        "latest", new TopicOffsetTimeStampMsgDeserialize())
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<KafkaManager.KafkaTopicOffsetTimeMsg>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                        .withTimestampAssigner(((element, recordTimestamp) -> element.ts())))
                        .setParallelism(2);

        SingleOutputStreamOperator resultStream =
                a
                        .connect(b)

                        .keyBy(KafkaManager.KafkaTopicOffsetTimeMsg::msg, KafkaManager.KafkaTopicOffsetTimeMsg::msg)

                        .process(new StreamConnectCoProcessFunc(rejectedWordsTag))
                        .setParallelism(2);


        resultStream.returns(Types.STRING).print();
        resultStream.getSideOutput(rejectedWordsTag).print();

    }
}
