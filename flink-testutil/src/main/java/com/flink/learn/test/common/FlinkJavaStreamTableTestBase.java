package com.flink.learn.test.common;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.TopicOffsetJsonEventtimeDeserialize;
import com.flink.common.deserialize.TopicOffsetMsgDeserialize;
import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.core.FlinkEvnBuilder;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
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
    public static  DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> baseKafkaSource;
    public static  DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> baseEventtimeKafkaSource;
    public static  DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> baseEventtimeJsonSource;


    @Before
    public void before() throws Exception {
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
        baseKafkaSource= getKafkaDataStream("test", "localhost:9092", "latest");
        baseEventtimeKafkaSource = getKafkaDataStreamWithEventTime("test", "localhost:9092", "latest");
        baseEventtimeJsonSource = getKafkaDataStreamWithJsonEventTime("test", "localhost:9092", "latest");

    }

    @After
    public void after() {
        System.out.println("Test End");
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

    public static Table getStreamTable(SingleOutputStreamOperator source, String fields) {
        return tableEnv.fromDataStream(source, fields);
    }


    /**
     *
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStream(String topic,
                                                      String broker,
                                                      String reset) {
        return streamEnv.addSource(getKafkaSource(topic, broker, reset));
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

    /**
     *
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStreamWithEventTime(String topic,
                                                      String broker,
                                                      String reset) {
        return streamEnv.addSource(getKafkaSourceWithEventtime(topic, broker, reset));
    }

    /**
     *
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static FlinkKafkaConsumer010<KafkaManager.KafkaTopicOffsetTimeMsg> getKafkaSourceWithEventtime(
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




    /**
     *
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static DataStreamSource getKafkaDataStreamWithJsonEventTime(String topic,
                                                                   String broker,
                                                                   String reset) {
        return streamEnv.addSource(getKafkaSourceWithJsonEventtime(topic, broker, reset));
    }

    /**
     *
     * @param topic
     * @param broker
     * @param reset
     * @return
     */
    public static FlinkKafkaConsumer010<KafkaManager.KafkaTopicOffsetTimeMsg> getKafkaSourceWithJsonEventtime(
            String topic,
            String broker,
            String reset) {
        FlinkKafkaConsumer010<KafkaManager.KafkaTopicOffsetTimeMsg> kafkasource = KafkaManager.getKafkaSource(
                topic,
                broker,
                new TopicOffsetJsonEventtimeDeserialize());
        kafkasource.setCommitOffsetsOnCheckpoints(true);
        if (reset == "earliest") {
            kafkasource.setStartFromEarliest(); //不加这个默认是从上次消费
        } else if (reset == "latest") {
            kafkasource.setStartFromLatest();
        }
        return kafkasource;
    }
}
