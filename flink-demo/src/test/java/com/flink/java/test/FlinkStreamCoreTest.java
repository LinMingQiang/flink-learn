package com.flink.java.test;

import com.flink.common.java.pojo.WordCountPoJo;
import com.flink.common.kafka.KafkaManager;
import com.flink.java.function.process.StreamConnectCoProcessFunc;
import com.flink.java.function.rich.AsyncIODatabaseRequest;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class FlinkStreamCoreTest extends FlinkJavaStreamTableTestBase {
    /**
     * 侧输出，效率更高；
     */
    @Test
    public void testOutputTag() throws Exception {
        DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> s1 = streamEnv.addSource(getKafkaSource("test", "localhost:9092", "latest"));
        OutputTag<WordCountPoJo> rejectedWordsTag = new OutputTag<WordCountPoJo>("rejected"){};

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

    /**
     * 异步io测试
     */
    @Test
    public void testAsyncIo() throws Exception {
        DataStreamSource<KafkaManager.KafkaTopicOffsetMsg> stream = streamEnv
                .addSource(getKafkaSource("test", "localhost:9092", "latest"));
        AsyncDataStream.unorderedWait(
                stream,
                new AsyncIODatabaseRequest(), 4, TimeUnit.SECONDS, 3) // 100异步最大个数，超过100个请求将构成反压。
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
    /**
     * 可用于双流join。假设 Test2为维表（永久保存状态）
     *
     * @throws Exception
     */
    @Test
    public void testConnectStream() throws Exception {
        // 10s过期
        OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected"){};
        SingleOutputStreamOperator<KafkaManager.KafkaTopicOffsetTimeMsg> sourceStream = streamEnv.addSource(
                getKafkaSourceWithTS("test", "localhost:9092", "latest"))
                        .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<KafkaManager.KafkaTopicOffsetTimeMsg>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(KafkaManager.KafkaTopicOffsetTimeMsg element) {
                        return element.ts();
                    }
                });
        DataStreamSource<KafkaManager.KafkaTopicOffsetTimeMsg> s2 = streamEnv.addSource(getKafkaSourceWithTS("test2", "localhost:9092", "latest"));
        SingleOutputStreamOperator resultStream = sourceStream.connect(s2)
                .keyBy(KafkaManager.KafkaTopicOffsetTimeMsg::msg, KafkaManager.KafkaTopicOffsetTimeMsg::msg)
                .process(new StreamConnectCoProcessFunc(rejectedWordsTag));


        resultStream.returns(Types.STRING).print();
        resultStream.getSideOutput(rejectedWordsTag).print();

        streamEnv.execute("");
        // .map(CoMapFunction<> value -> null);
    }

}
