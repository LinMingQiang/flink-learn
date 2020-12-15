package com.flink.learn.test.common;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.deserialize.TopicOffsetJsonEventtimeDeserialize;
import com.flink.common.deserialize.TopicOffsetMsgDeserialize;
import com.flink.common.deserialize.TopicOffsetTimeStampMsgDeserialize;
import com.flink.common.java.core.FlinkEvnBuilder;
import com.flink.common.java.core.FlinkSourceBuilder;
import com.flink.common.java.core.FlinkSourceBuilder.*;
import com.flink.common.kafka.KafkaManager;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;
import java.util.Arrays;

public class FlinkJavaStreamTableTestBase extends FlinkSourceBuilder implements Serializable {

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

}
