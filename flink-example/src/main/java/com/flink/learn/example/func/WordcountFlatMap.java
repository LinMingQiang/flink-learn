package com.flink.learn.example.func;

import com.flink.learn.example.entry.WordCount;
import com.flink.learn.example.metric.WordcountMetric;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class WordcountFlatMap extends RichFlatMapFunction<String, WordCount.WordWithCount> {
    public Map<String, String> cache = new HashMap<>();
    WordcountMetric metric ;
    @Override
    public void flatMap(String s, Collector<WordCount.WordWithCount> collector) throws Exception {
        metric.incRequestTimes();
        for (String word : s.split("\\s")) {
            metric.incTotalRequestTimes();
            cache.put(word, word);
            collector.collect(new WordCount.WordWithCount(word, 1L));
        }
    }

    public int getCacheSize(){
        return cache.size();
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        metric = new WordcountMetric(getRuntimeContext().getMetricGroup());
        metric.initCacheSizeGauge(this::getCacheSize);
//        getRuntimeContext().getMetricGroup().gauge("noGroupGauge-stmp", () -> System.currentTimeMillis());
//        getRuntimeContext().getMetricGroup().gauge("noGroupGauge-inputcount", () -> inputcount );
//        getRuntimeContext().getMetricGroup().gauge("noGroupGauge-outcount", () -> outcount);
//
//        getRuntimeContext().getMetricGroup().addGroup("group-key","group-value").gauge("GroupGauge-stmp", () -> System.currentTimeMillis());
//        getRuntimeContext().getMetricGroup().addGroup("group-key","group-value").gauge("GroupGauge-inputcount", () -> inputcount );
//        getRuntimeContext().getMetricGroup().addGroup("group-key","group-value").gauge("GroupGauge-outcount", () -> outcount);

    }
}
