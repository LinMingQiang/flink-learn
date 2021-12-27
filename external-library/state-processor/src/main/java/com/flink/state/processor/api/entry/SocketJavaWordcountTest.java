package com.flink.state.processor.api.entry;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.state.processor.api.pojo.WordCountGroupByKey;
import com.flink.state.processor.api.pojo.WordCountPoJo;
import com.flink.state.processor.api.sink.WordCountJavaSink;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;

public class SocketJavaWordcountTest {
    public static void main(String[] args) throws Exception {
        FlinkLearnPropertiesUtil.init(EnvironmentalKey.LOCAL_PROPERTIES_PATH(),
                "SocketJavaPoJoWordcountTest");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000L); //更新offsets。每60s提交一次
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(FlinkLearnPropertiesUtil.CHECKPOINT_PATH(), true);
        env.setStateBackend(rocksDBStateBackend);
        // nc -lk 9877
        DataStream<String> source = env.socketTextStream("localhost", 9877);
        // tuple2 和 WordCountGroupByKey 是类似的，tuple2不需要自己去实现hashcode和equal方法
        source
                .map(x ->
                        new WordCountPoJo(x, 1L, new Date().getTime(), x.split(","), new WordCountGroupByKey(x))
                )
                // WordCountGroupByKey 做keyby
                .keyBy((KeySelector<WordCountPoJo, WordCountGroupByKey>) value ->  new WordCountGroupByKey(value.word))
                // 统计每个单词的count数
                .flatMap(new RichFlatMapFunction<WordCountPoJo, WordCountPoJo>() {
                    ValueState<WordCountPoJo> lastState = null;
                    @Override
                    public void flatMap(WordCountPoJo value, Collector<WordCountPoJo> out) throws Exception {
                        WordCountPoJo ls = lastState.value();
                        if (ls == null) {
                            ls = value;
                        } else {
                            ls.count += value.count;
                        }
                        lastState.update(ls);
                        out.collect(ls);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        ValueStateDescriptor desc = new ValueStateDescriptor<WordCountPoJo>(
                                "wordcountState", WordCountPoJo.class);
                        lastState = getRuntimeContext().getState(desc);
                    }
                })
                .name("flatMapUID")
                .uid("flatMapUID")
                .addSink(new WordCountJavaSink())
                .uid("sink");
        env.execute("SocketWordcountTest");
    }
}
