package com.flink.java.test;

import com.flink.common.kafka.KafkaManager.KafkaTopicOffsetMsg;
import com.flink.learn.richf.WordCountRichFunction;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.util.Collector;
import org.junit.Test;


/**
 * 基本算子的使用
 */
public class FlinkCoreOperatorTest extends FlinkJavaStreamTableTestBase {

    @Test
    public void testWordCount() throws Exception {
        DataStreamSource<KafkaTopicOffsetMsg> s1 = getKafkaDataStream("test", "localhost:9092", "latest");
        s1
                .flatMap((FlatMapFunction<KafkaTopicOffsetMsg, String>) (value, out) -> {
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

        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}
