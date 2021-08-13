package com.flink.learn.entry;

import com.core.FlinkEvnBuilder;
import com.flink.common.deserialize.KafkaMessageDeserialize;
import com.flink.common.kafka.KafkaManager;
import com.manager.KafkaSourceManager;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class WordCountTest {
    public static void main(String[] args) throws Exception {
        try {
            FlinkEvnBuilder.initEnv("application.properties");
            runWordCount();
            new Thread(() -> {
                try {
                    System.out.println(">>>>>>>>>>");
                    Thread.sleep(60000L);
                    runWordCount();
                    FlinkEvnBuilder.streamEnv.execute("WordCountTest_2");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            FlinkEvnBuilder.streamEnv.execute("WordCountTest_1");

        } catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(">>>>>>>>>>>.e nd ");
    }

    /**
     * wordcount
     */
    public static void runWordCount() {
        DataStreamSource<KafkaManager.KafkaMessge> s1 =
                KafkaSourceManager.getKafkaDataStream(FlinkEvnBuilder.streamEnv,
                        "test",
                        "10.21.33.28:29092,10.21.33.29:29092,10.21.131.11:29092",
                        "latest",
                        new KafkaMessageDeserialize());
        s1
                .flatMap((FlatMapFunction<KafkaManager.KafkaMessge, String>) (value, out) -> {
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
