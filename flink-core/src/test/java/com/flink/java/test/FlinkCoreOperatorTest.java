package com.flink.java.test;

import com.flink.common.core.FlinkEvnBuilder;
import com.pojo.WordCountPoJo;
import com.flink.common.kafka.KafkaManager;
import com.flink.common.kafka.KafkaManager.KafkaMessge;
import com.func.processfunc.StreamConnectCoProcessFunc;
import com.func.richfunc.AsyncIODatabaseRequest;
import com.flink.learn.test.common.FlinkJavaStreamTableTestBase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/**
 * 基本算子的使用
 */
public class FlinkCoreOperatorTest extends FlinkJavaStreamTableTestBase {

    /**
     * wordcount
     *
     * @throws Exception
     */
    @Test
    public void testWordCount() throws Exception {

        streamEnv.setParallelism(3);
        // {"msg":"hello"}
        kafkaDataSource
//                .flatMap((FlatMapFunction<KafkaMessge, Tuple2<String, Long>>) (value, out) -> {
//                    for (String s : value.msg().split(",", -1)) {
//                        out.collect(new Tuple2<String, Long>(s, 1L));
//                    }
//                })
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .setParallelism(3)
//                .returns(Types.STRING)
//                .map(x -> new Tuple2<String, Long>(x, 1L))
//                .setParallelism(3)
//                .returns(Types.TUPLE(Types.STRING, Types.LONG))
//                .filter(x -> x.f1 >= 0)
//                .setParallelism(2)
                .keyBy(x-> x.msg())
                .sum(1)
//                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Tuple2<String, Long>>() {
//                    ValueState<Long> v;
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        v = getRuntimeContext().getState(
//                                new ValueStateDescriptor("test2",
//                                        TypeInformation.of(Long.class)));
//                    }
//                    @Override
//                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
//                        ctx.getCurrentKey();
//                        Long re = v.value();
//                        if (re != null) {
//                            re += value.f1;
//                        } else re = value.f1;
//                        v.update(re);
//                        out.collect(new Tuple2<>(value.f0, re));
//                    }
//                })
////                .sum(1)
//                .setParallelism(2)
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名, 一个execute是一个job
    }

    /**
     * 侧边输出，将过滤的数据从另外一个sink输出
     */
    @Test
    public void testOutputTag() throws Exception {
        OutputTag<WordCountPoJo> rejectedWordsTag = new OutputTag<WordCountPoJo>("rejected") {
        };
        SingleOutputStreamOperator<WordCountPoJo> sourceStream = kafkaDataSource
                .process(new KeyedProcessFunction<String, KafkaMessge, WordCountPoJo>() {
                    @Override
                    public void processElement(KafkaMessge value, Context ctx, Collector<WordCountPoJo> out) {
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
        System.out.println(streamEnv.getExecutionPlan());
        streamEnv.execute("testOutputTag");
    }

    /**
     * watermark是广播发送的，watermark是广播发送的，watermark是广播发送的
     * 可用于双流join。假设 Test2为维表（永久保存状态）
     * 双流connect 必须都有watermark，否则一个产生不了watermark，不会触发 registerEventimeTimer
     * 输入a: a1 a2 a3 ，a流的wtm = a3 。但是因为是双流 ，所以wtm取最小的 = b = min
     * 再输入b : b2 b3 ， b流的wtm = b3 。  最后的wtm = min (a3, b3) = (a3 - 10s)
     * 这个时候才触发 a的过期，a1,a2 ，如果已经超过10s了的话
     * 后面再输入其他的，就去 最小的那个。每次输入，各自流都会更新自己的wtm，然后再跟另一个比较取最小
     *
     * @throws Exception
     */
    @Test
    public void testConnectStream() throws Exception {
        // 10s过期
        OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected") {
        };

        SingleOutputStreamOperator resultStream =
                kafkaDataSource
                        .connect(getKafkaKeyStream("test2", "localhost:9092", "latest"))
                        .keyBy(KafkaMessge::msg, KafkaMessge::msg)
                        .process(new StreamConnectCoProcessFunc(rejectedWordsTag))
                        .setParallelism(4);


        resultStream.returns(Types.STRING).print();
        resultStream.getSideOutput(rejectedWordsTag).print();

        streamEnv.execute("");
    }

    /**
     * 异步io测试
     */
    @Test
    public void testAsyncIo() throws Exception {
        AsyncDataStream.unorderedWait(
                kafkaDataSource,
                new AsyncIODatabaseRequest(),
                4,
                TimeUnit.SECONDS,
                3) // 100异步最大个数，超过100个请求将构成反压。
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }


    /**
     * 异步io测试
     */
    @Test
    public void testTtl() throws Exception {
        streamEnv.setParallelism(1);
        // {"msg":"hello"}
        kafkaDataSource
                .flatMap((FlatMapFunction<KafkaMessge, String>) (value, out) -> {
                    for (String s : value.msg().split(",", -1)) {
                        out.collect(s);
                    }
                })
                .returns(Types.STRING)
                .map(x -> new Tuple2<String, Long>(x, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(x -> x.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, String>() {
                    ValueState<Long> inc = null;

                    // 这里面的非 key state是 task公用的。这里面的Operator state也是task公用的。
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor desc = new ValueStateDescriptor<Long>("inc", Types.LONG, 0L);
                        desc.enableTimeToLive(FlinkEvnBuilder.getStateTTLConf(1)); // 1分钟过期
                        inc = getRuntimeContext().getState(desc);
                    }

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + 10000);
                        Long v = inc.value();
                        if (v != null) {
                            v = value.f1 + v;
                        } else {
                            v = value.f1;
                        }
                        inc.update(v);
                        out.collect("" + v);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                    }
                })
                .setParallelism(1)
                .print();
        streamEnv.execute("lmq-flink-demo"); //程序名
    }
}
