package com.core;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.*;
import com.flink.common.kafka.KafkaManager.KafkaMessge;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSourceBuilder extends FlinkStreamEnvAndSource {

    /**
     * 对于不是测试类，需要手动init
     * @throws IOException
     */
    public static void init() throws IOException {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "local-test");
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        tableEnv = FlinkEvnBuilder.buildStreamTableEnv(
                streamEnv,
                Duration.ofHours(2));
        kafkaDataSource = getKafkaKeyStream("test", "localhost:9092", "latest");
        kafkaDataTable = getStreamTable(kafkaDataSource,
                $("topic"),
                $("offset"),
                $("ts"),
                $("msg"),
                $("rowtime"),
                $("uid"));
//        baseEventtimeKafkaSource = getKafkaDataStreamWithEventTime("test", "localhost:9092", "latest");
//        baseEventtimeJsonSource = getKafkaDataStreamWithJsonEventTime("test", "localhost:9092", "latest");
//        baseEventtimeJsonUidMsgSource = getKafkaDataStreamWithJsonUidMsg("test", "localhost:9092", "latest");
//        baseReqImpClickSource = getKafkaDataStreamReqImpclickMsg("test", "localhost:9092", "latest",
//                new TopicOffsetJsonReqImpClickDeserialize());
    }


    /**
     * 经过keyby
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static KeyedStream<KafkaMessge, String> getKafkaKeyStream(
            String topic,
            String broker,
            String reset) {
        return getKafkaDataStreamSource(topic, broker, reset).keyBy(KafkaMessge::msg);
    }
    /**
     * 没有keyby
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static SingleOutputStreamOperator<KafkaMessge> getKafkaDataStreamSource(
            String topic,
            String broker,
            String reset) {
        return streamEnv.addSource(getKafkaConsumer(topic, broker, reset))
                        .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<KafkaMessge>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                                .withTimestampAssigner(((element, recordTimestamp) -> element.ts()))
                );
    }

    /**
     * 获取
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static FlinkKafkaConsumer<KafkaMessge> getKafkaConsumer(String topic,
                                                                   String broker,
                                                                   String reset) {
        FlinkKafkaConsumer<KafkaMessge> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new KafkaMessageDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }

    public static Table getStreamTable(KeyedStream<KafkaMessge, String> source, Expression... fields) {
        return tableEnv.fromDataStream(source, fields);
    }

    public static Table getStreamTable(String topic,
                                       String broker,
                                       String reset) {
        return getStreamTable(getKafkaKeyStream(topic, broker, reset),
                $("topic"),
                $("offset"),
                $("ts"),
                $("msg"),
                $("rowtime"),
                $("uid"));
    }

    public static Table getStreamTable(String topic,
                                       String broker,
                                       String reset,
                                       Expression... fields) {
        return getStreamTable(getKafkaKeyStream(topic, broker, reset), fields);
    }

    public static Table getStreamTable(SingleOutputStreamOperator source, Expression... fields) {
        return tableEnv.fromDataStream(source, fields);
    }

    public static void printlnStringTable(Table b) {
        tableEnv.toRetractStream(b,
                Row.class)
                .print();
    }
}
