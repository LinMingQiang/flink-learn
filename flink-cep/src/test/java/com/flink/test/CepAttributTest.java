package com.flink.test;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import com.flink.common.kafka.KafkaManager.KafkaMessge;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.junit.Test;

public class CepAttributTest extends FlinkJavaStreamTableTestBase {
    /**
     * 模仿 请求-》曝光-》点击 outputTag输出没有匹配到最后的结果 within 是各个pattern的等待时间
     *
     * @throws Exception
     */
    @Test
    public void testReqImpClick() throws Exception {
        // {"ts":1,"log":"req","reqid":"1","msg":"start"}
        // {"ts":2,"log":"req","reqid":"1","msg":"start"}
        // {"ts":3,"log":"req","reqid":"1","msg":"middle"}
        // {"ts":4,"log":"req","reqid":"1","msg":"middle"}
        // {"ts":5,"log":"req","reqid":"1","msg":"end"} {"ts":6,"log":"req","reqid":"1","msg":"end"}
        // 只要通过一次，后面再来 end也没用
        //  AfterMatchSkipStrategy.skipPastLastEvent() 只保留第一个匹配结果
        Pattern p =
                Pattern.<KafkaMessge>begin("start")
                        .where(
                                new SimpleCondition<KafkaMessge>() {
                                    @Override
                                    public boolean filter(KafkaMessge kafkaTopicOffsetTimeMsg)
                                            throws Exception {
                                        return kafkaTopicOffsetTimeMsg.msg().equals("start");
                                    }
                                })
                        .followedBy("middle")
                        .where(
                                new SimpleCondition<KafkaMessge>() {
                                    @Override
                                    public boolean filter(KafkaMessge kafkaTopicOffsetTimeMsg)
                                            throws Exception {
                                        return kafkaTopicOffsetTimeMsg.msg().equals("middle");
                                    }
                                })
                        .within(Time.seconds(50))
                        .followedBy("end")
                        .where(
                                new SimpleCondition<KafkaMessge>() {
                                    @Override
                                    public boolean filter(KafkaMessge kafkaTopicOffsetTimeMsg)
                                            throws Exception {
                                        return kafkaTopicOffsetTimeMsg.msg().equals("end");
                                    }
                                })
                        .within(Time.seconds(50));

        OutputTag<String> out = new OutputTag<String>("late") {};
        SingleOutputStreamOperator<String> flatResult =
                CEP.pattern(getKafkaDataStreamSource("", "", "").keyBy(x -> x.topic()), p)
                        .inProcessingTime() // 每来一条就触发，eventtime的话就是更加watermark来触发
                        .select(
                                out,
                                (PatternTimeoutFunction<KafkaMessge, String>)
                                        (map, l) -> map.toString(),
                                (PatternSelectFunction<KafkaMessge, String>) map -> map.toString());
        flatResult.print();
        DataStream<String> timeoutResult = flatResult.getSideOutput(out);
        timeoutResult.map(x -> "out : " + x).print();

        streamEnv.execute();
    }
}
