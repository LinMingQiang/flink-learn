package com.flink.java.test;

import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.common.kafka.KafkaManager;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

public class FlinkStreamCoreTest extends FlinkJavaStreamTableTestBase {
    /**
     * 侧输出，效率更高；
     */
    @Test
    public void testOutputTag() throws Exception {
        DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> s1 = streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"));
        OutputTag<WordCountPoJo> rejectedWordsTag = new OutputTag<WordCountPoJo>("rejected") {
        };

        SingleOutputStreamOperator<WordCountPoJo> sourceStream = s1.keyBy(x -> x.msg())
                .process(new KeyedProcessFunction<String, KafkaManager.KafkaTopicOffsetMsg, WordCountPoJo>() {
                    @Override
                    public void processElement(KafkaManager.KafkaTopicOffsetMsg value, Context ctx, Collector<WordCountPoJo> out) throws Exception {
                        if (!value.msg().matches("^[0-9]*$")) {
                            ctx.output(rejectedWordsTag, new WordCountPoJo("rejested", 1));
                        } else {
                            out.collect(new WordCountPoJo(value.msg(), 1));
                        }
                    }
                })
                .returns(WordCountPoJo.class);
        // 正常的数字输出
        sourceStream.keyBy(x -> x.word)
                .sum("num")
                .print();
        // 错误数据输出
        sourceStream
                .getSideOutput(rejectedWordsTag)
                .keyBy(x -> x.word)
                .sum("num")
                .print();
        streamEnv.execute("ww");

    }
}
