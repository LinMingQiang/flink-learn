package com.flink.learn.entry;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import com.core.FlinkEvnBuilder;
import com.core.FlinkSourceBuilder;
import com.core.FlinkStreamEnvAndSource;
import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.common.kafka.KafkaManager.KafkaMessge;

import java.util.List;
import java.util.Map;

public class CepDemoEntry extends FlinkStreamEnvAndSource {

    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(), "CepDemoEntry");
        streamEnv =
                FlinkEvnBuilder.buildStreamingEnv(
                        FlinkLearnPropertiesUtil.param(),
                        FlinkLearnPropertiesUtil.CHECKPOINT_PATH(),
                        FlinkLearnPropertiesUtil.CHECKPOINT_INTERVAL());

        SingleOutputStreamOperator<KafkaMessge> d1 =
                FlinkSourceBuilder.getKafkaDataStreamSource("test", "localhost:9092", "latest");
        Pattern<KafkaMessge, KafkaMessge> req_imp =
                Pattern.<KafkaMessge>begin("start")
                        .where(
                                new SimpleCondition<KafkaMessge>() {
                                    @Override
                                    public boolean filter(KafkaMessge kafkaTopicOffsetTimeMsg)
                                            throws Exception {
                                        return kafkaTopicOffsetTimeMsg.msg().equals("start");
                                    }
                                })
                        .next("middle") // 接着有imp
                        .where(
                                new SimpleCondition<KafkaMessge>() {
                                    @Override
                                    public boolean filter(KafkaMessge kafkaTopicOffsetTimeMsg)
                                            throws Exception {
                                        System.out.println("middle");
                                        return kafkaTopicOffsetTimeMsg.msg().equals("middle");
                                    }
                                })
                //                        .followedByAny("end") // 接着有imp
                //                        .where(new SimpleCondition<KafkaTopicOffsetTimeMsg>() {
                //                            @Override
                //                            public boolean filter(KafkaTopicOffsetTimeMsg
                // kafkaTopicOffsetTimeMsg) throws Exception {
                //                                System.out.println("end");
                //                                return
                // kafkaTopicOffsetTimeMsg.msg().equals("end");
                //                            }
                //                        })
                ;

        CEP.pattern(d1, req_imp)
                .inProcessingTime()
                .select(
                        new PatternSelectFunction<KafkaMessge, String>() {
                            @Override
                            public String select(Map<String, List<KafkaMessge>> map)
                                    throws Exception {
                                return map.toString();
                            }
                        })
                .print();

        streamEnv.execute();
    }
}
