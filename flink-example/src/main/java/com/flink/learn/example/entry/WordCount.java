package com.flink.learn.example.entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {
    // nc -l 9877
    // flink run -c com.flink.learn.example.entry.WordCount /Users/eminem/workspace/flink/flink-learn/flink-example/target/flink-example-1.13.0.jar
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000L); //更新offsets。每60s提交一次
        // 表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///Users/eminem/workspace/flink/flink-learn/checkpoint/WordCount", false);
        rocksDBStateBackend.setDbStoragePath("/Users/eminem/workspace/flink/flink-learn/checkpoint/rocksdb-tmp-file");
        env.setStateBackend(rocksDBStateBackend);

        DataStream<String> text = env.socketTextStream("localhost", 9877, "\n");
        DataStream<WordWithCount> windowCounts =
                text.flatMap(new RichFlatMapFunction<String, WordWithCount>() {
                            long inputcount = 0L;
                            long outcount=0L;
                            @Override
                            public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
                                inputcount+=1;
                                for (String word : s.split("\\s")) {
                                    outcount+=1;
                                    collector.collect(new WordWithCount(word, 1L));
                                }
                            }

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                getRuntimeContext().getMetricGroup().gauge("noGroupGauge-stmp", () -> System.currentTimeMillis());
                                getRuntimeContext().getMetricGroup().gauge("noGroupGauge-inputcount", () -> inputcount );
                                getRuntimeContext().getMetricGroup().gauge("noGroupGauge-outcount", () -> outcount);

                                getRuntimeContext().getMetricGroup().addGroup("group-key","group-value").gauge("GroupGauge-stmp", () -> System.currentTimeMillis());
                                getRuntimeContext().getMetricGroup().addGroup("group-key","group-value").gauge("GroupGauge-inputcount", () -> inputcount );
                                getRuntimeContext().getMetricGroup().addGroup("group-key","group-value").gauge("GroupGauge-outcount", () -> outcount);

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
