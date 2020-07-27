package com.flink.java.test;

import com.flink.common.kafka.KafkaManager.*;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class FlinkStreamConnectTest extends FlinkJavaStreamTableTestBase {

    /**
     * 可用于双流join。
     * @throws Exception
     */
    @Test
    public void testConnectStream() throws Exception {
        DataStreamSource<KafkaTopicOffsetMsg> s1 = streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"));
        DataStreamSource<KafkaTopicOffsetMsg> s2 = streamEnv.addSource(getKafkaSource("test2", "localhost:9092", "latest"));
        s1.connect(s2)
                .keyBy(KafkaTopicOffsetMsg::msg, KafkaTopicOffsetMsg::msg)

                .process(new KeyedCoProcessFunction<Object, KafkaTopicOffsetMsg, KafkaTopicOffsetMsg, String>() {
                    ValueState<KafkaTopicOffsetMsg> v1 = null;
                    ValueState<KafkaTopicOffsetMsg> v2 = null;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        v1 = getRuntimeContext().getState(
                                new ValueStateDescriptor<KafkaTopicOffsetMsg>("test1",
                                        TypeInformation.of(KafkaTopicOffsetMsg.class)));
                        v2 = getRuntimeContext().getState(
                                new ValueStateDescriptor<KafkaTopicOffsetMsg>("test2",
                                        TypeInformation.of(KafkaTopicOffsetMsg.class)));
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);

                    }

                    @Override
                    public void processElement1(KafkaTopicOffsetMsg value, Context ctx, Collector<String> out) throws Exception {

                    }

                    @Override
                    public void processElement2(KafkaTopicOffsetMsg value, Context ctx, Collector<String> out) throws Exception {

                    }
                })
                .returns(Types.STRING)
                .print();
//                .map(new CoMapFunction<KafkaTopicOffsetMsg, KafkaTopicOffsetMsg, String>() {
//
//                    @Override
//                    public String map1(KafkaTopicOffsetMsg value) throws Exception {
//                        return "t1 : "+value;
//                    }
//
//                    @Override
//                    public String map2(KafkaTopicOffsetMsg value) throws Exception {
//                        return null;
//                    }
//                })
//                .returns(Types.STRING)
//                .print();
        streamEnv.execute("");
        // .map(CoMapFunction<> value -> null);
    }
}
