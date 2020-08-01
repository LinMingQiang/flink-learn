package com.flink.learn.test.common;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.TopicOffsetMsgDeserialize;
import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.core.FlinkEvnBuilder;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;
import java.util.Arrays;

public class FlinkJavaStreamTableTestBase extends AbstractTestBase implements Serializable {
    public static StreamTableEnvironment tableEnv = null;
    public static TableEnvironment tableE = null;
    public static StreamExecutionEnvironment streamEnv = null;
    public static ExecutionEnvironment bEnv = null;

    @Before
    public void before() throws Exception {
        System.out.println("! INITIALIZE ExecutionEnvironment SUCCESS !");
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "FlinkLearnStreamDDLSQLEntry");
        bEnv = ExecutionEnvironment.getExecutionEnvironment();
        streamEnv = FlinkEvnBuilder.buildStreamingEnv(FlinkLearnPropertiesUtil.param(),
                FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());
        tableEnv = FlinkEvnBuilder.buildStreamTableEnv(streamEnv,
                Time.minutes(1),
                Time.minutes(6));

        tableE = FlinkEvnBuilder.buildTableEnv();
    }

    @After
    public void after() {
        System.out.println("<<<<<<<<<<<<<<<<<<<");
    }


    public static FlinkKafkaConsumer010<KafkaManager.KafkaTopicOffsetMsg> getKafkaSource(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer010<KafkaManager.KafkaTopicOffsetMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetMsgDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }


    public static FlinkKafkaConsumer010<KafkaManager.KafkaTopicOffsetTimeMsg> getKafkaSourceWithTS(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer010<KafkaManager.KafkaTopicOffsetTimeMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetTimeStampMsgDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }


    public static Table getStreamTable(DataStreamSource source, String fields) {
        return tableEnv.fromDataStream(source, fields);
    }

    public static DataStreamSource getKafkaDataStream(String topic,
                                                      String broker,
                                                      String reset) {
        return streamEnv.addSource(getKafkaSource(topic, broker, reset));
    }
}
