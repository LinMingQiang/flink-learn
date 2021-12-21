package com.flink.learn.example.entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {
    // nc -l 9877
    // flink run -c com.flink.learn.example.entry.WordCount /Users/eminem/workspace/flink/flink-learn/flink-example/target/flink-example-1.13.0.jar
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9877, "\n");
        DataStream<WordWithCount> windowCounts =
                text.flatMap((FlatMapFunction<String, WordWithCount>) (value, out) -> {
                                    for (String word : value.split("\\s")) {
                                        out.collect(new WordWithCount(word, 1L));
                                    }
                                })
                        .returns(WordWithCount.class)
                        .keyBy(value -> value.word)
                        .sum("count");
        windowCounts.print().setParallelism(1);
        env.execute("WordCountJobName");
    }
    /** Data type for words with count. */
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
