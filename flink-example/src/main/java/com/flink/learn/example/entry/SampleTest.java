package com.flink.learn.example.entry;

import com.flink.learn.example.pojo.WordWithCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SampleTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9877, "\n");
        SingleOutputStreamOperator<WordWithCount> windowCounts =
                text.flatMap((FlatMapFunction<String, WordWithCount>) (value, out) ->
                                out.collect(new WordWithCount(value, 1L))
                        )
                        .returns(WordWithCount.class)
//                        .keyBy(value -> value.word)
//                        .sum("count")
                ;
        windowCounts.print();
        windowCounts.print();
        env.execute("WordCountJobName");
    }
}
