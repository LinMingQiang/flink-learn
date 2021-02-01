package com.flink.learn.stateprocessor;

import com.flink.common.core.EnvironmentalKey;
import com.flink.common.core.FlinkLearnPropertiesUtil;
import com.flink.learn.bean.WordCountGroupByKey;
import com.flink.learn.bean.WordCountPoJo;
import com.flink.learn.sink.WordCountJavaSink;
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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //env.setStateBackend(new FsStateBackend(checkpointPath))
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(FlinkLearnPropertiesUtil.CHECKPOINT_PATH(), true);
        // rocksDBStateBackend.enableTtlCompactionFilter(); // 启用ttl后台增量清除功能
        env.setStateBackend(rocksDBStateBackend);

        DataStream<String> source = env.socketTextStream("localhost", 9877);
        source
                .map(x ->
                        new WordCountPoJo(x, 1L, new Date().getTime(), x.split(","), new WordCountGroupByKey(x))
                )
                .keyBy(new KeySelector<WordCountPoJo, WordCountGroupByKey>() {
                    // tuple2 和 WordCountGroupByKey 是类似的，tuple2不需要自己去实现hashcode和equal方法
                    @Override
                    public WordCountGroupByKey getKey(WordCountPoJo value) throws Exception {
                        // return new Tuple2<String, String>(value.word, value.word);
                        // return value.word;
                        return new WordCountGroupByKey(value.word);
                    }
                })
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
                .name("wordcountUID")
                .uid("wordcountUID")
                .addSink(new WordCountJavaSink())
                .uid("wordcountsink");

        // .print();
        env.execute("SocketWordcountTest");
    }
}
