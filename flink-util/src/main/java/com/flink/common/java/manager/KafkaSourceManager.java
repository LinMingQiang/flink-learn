package com.flink.common.java.manager;

import com.flink.common.deserialize.TopicOffsetJsonEventtimeDeserialize;
import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaSourceManager {

    private static <T> FlinkKafkaConsumer010<T> getKafkaSource(
            String topic,
            String broker,
            String reset,
            KafkaDeserializationSchema<T> kds) {
        FlinkKafkaConsumer010<T> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                kds);
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }


    public static <T> DataStreamSource getKafkaDataStream(StreamExecutionEnvironment streamEnv,
                                                          String topic,
                                                          String broker,
                                                          String reset,
                                                          KafkaDeserializationSchema<T> kds) {
        return streamEnv.addSource(getKafkaSource(topic, broker, reset, kds));
    }

    public static Table getStreamTable(StreamTableEnvironment streamTableEnv, DataStreamSource source ,String fields) {
        return streamTableEnv.fromDataStream(source, fields);
    }


}
