package com.flink.learn.example.entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.flink.learn.example.pojo.WordWithCount;

public class SampleTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9877, "\n");
        SingleOutputStreamOperator<WordWithCount> windowCounts =
                text.flatMap(
                                (FlatMapFunction<String, WordWithCount>)
                                        (value, out) -> out.collect(new WordWithCount(value, 1L)))
                        .returns(WordWithCount.class)
                //                        .keyBy(value -> value.word)
                //                        .sum("count")
                ;
        windowCounts.print();
        windowCounts.print();
        env.execute("WordCountJobName");
    }
}
