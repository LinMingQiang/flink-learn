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
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class WordCountTest {

    /**
     * 1： 正常模式下会生成多个applicationid，这个看源码能知道，在execute里面去申请appliation的，所有每次execute都会生成app
     * 2：使用yarn-application模式，可以执行多次的execute而不会生成多个applicationid
     * 3：使用sesison可以达到application模式一样的效果
     * 可以动态地提交多个job，用session模式也行，但是session得用脚本启动
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        try {
            FlinkEvnBuilder.initEnv("application.properties");
            connectWc();
//            new Thread(() -> {
//                try {
//                    System.out.println(">>>>>>>>>>");
//                    Thread.sleep(60000L);
//                    runWordCount();
//                    FlinkEvnBuilder.streamEnv.execute("WordCountTest_2");
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }).start();
            FlinkEvnBuilder.streamEnv.execute("WordCountTest_1");

        } catch (Exception e) {
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
                        "localhost:9092",
                        "latest",
                        new KafkaMessageDeserialize());
        s1
                .flatMap((FlatMapFunction<KafkaManager.KafkaMessge, String>) (value, out) -> {
                    for (String s : value.msg().split(",", -1)) {
                        out.collect(s);
                    }
                })
                .setParallelism(5)
                .returns(Types.STRING)
                .map(x -> new Tuple2(x, 1L))
                .setParallelism(4)
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .sum(1)
                .setParallelism(3)
                .print()
                .setParallelism(1);
    }

    public static void connectWc() {
        DataStreamSource<KafkaManager.KafkaMessge> s1 =
                KafkaSourceManager.getKafkaDataStream(FlinkEvnBuilder.streamEnv,
                        "test",
                        "localhost:9092",
                        "latest",
                        new KafkaMessageDeserialize()).setParallelism(3);

        DataStreamSource<KafkaManager.KafkaMessge> s2 =
                KafkaSourceManager.getKafkaDataStream(FlinkEvnBuilder.streamEnv,
                        "test2",
                        "localhost:9092",
                        "latest",
                        new KafkaMessageDeserialize()).setParallelism(6);

        s1.connect(s2)
                .keyBy(x->x.msg(), x -> x.msg())
                .flatMap(new CoFlatMapFunction<KafkaManager.KafkaMessge, KafkaManager.KafkaMessge, String>() {
                    @Override
                    public void flatMap1(KafkaManager.KafkaMessge value, Collector<String> out) throws Exception {
                        for (String s : value.msg().split(",", -1)) {
                            out.collect(s);
                        }
                    }

                    @Override
                    public void flatMap2(KafkaManager.KafkaMessge value, Collector<String> out) throws Exception {
                        for (String s : value.msg().split(",", -1)) {
                            out.collect(s);
                        }
                    }
                })
                .setParallelism(2)
                .keyBy(x -> x)
                .print()
                .setParallelism(1);
    }
}
