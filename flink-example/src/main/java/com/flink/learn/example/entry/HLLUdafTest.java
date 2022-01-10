package com.flink.learn.example.entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HLLUdafTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 9877, "\n");
        EnvironmentSettings sett = EnvironmentSettings.newInstance().build();
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env, sett);
        Configuration configuration = streamTableEnv.getConfig().getConfiguration();
        configuration.setString(
                "table.exec.mini-batch.enabled", "true"); // enable mini-batch optimization
        configuration.setString(
                "table.exec.mini-batch.allow-latency",
                "5 s"); // use 5 seconds to buffer input records
        configuration.setString("table.exec.mini-batch.size", "5000");
        configuration.setString(
                "table.optimizer.agg-phase-strategy",
                "TWO_PHASE"); // enable two-phase, i.e. local-global aggregation

        streamTableEnv.executeSql(
                "CREATE FUNCTION hll_distinct AS 'com.hll.FlinkUDAFCardinalityEstimationFunction'");

        DataStream<WordWithCount> windowCounts =
                text.flatMap(
                                (FlatMapFunction<String, WordWithCount>)
                                        (s, collector) -> {
                                            for (String word : s.split("\\s")) {
                                                collector.collect(new WordWithCount(word, 1L));
                                            }
                                        })
                        .returns(WordWithCount.class);
        streamTableEnv.createTemporaryView("test", windowCounts);
        String selectSql = "select word,hll_distinct(word) cnt from test group by word";
        streamTableEnv.toRetractStream(streamTableEnv.sqlQuery(selectSql), Row.class).print();
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
