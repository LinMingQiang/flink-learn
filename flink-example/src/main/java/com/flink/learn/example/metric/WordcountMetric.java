package com.flink.learn.example.metric;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

public class WordcountMetric {
    private MetricGroup metricGroup;

    private static final String METRIC_GROUP = "wordCountMetric";
    private static final String TPS_METRIC_GAUGE = "tps";
    // 总请求次数。用来记录join 的频率，分析是否数据倾斜
    private static final String TOTAL_WORD_NUMS_METRIC_GAUGE = "totalWordNums";
    private static final String CACHE_SIZE_METRIC_GAUGE = "cacheSize";

    // 计算速率 ： 当前批次数据量 / 用时
    private Long requestTimes = 0L;
    private Long lastCalculateTimestamp = 0L;
    private Long totalWordNums = 0L;

    public WordcountMetric(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        metricGroup
                .addGroup(METRIC_GROUP)
                .gauge(TPS_METRIC_GAUGE, this::calculateTps);

        metricGroup
                .addGroup(METRIC_GROUP)
                .gauge(TOTAL_WORD_NUMS_METRIC_GAUGE, () -> totalWordNums);
    }

    /**
     * 计算 tps
     * @return tps
     */
    public Double calculateTps(){
        if(requestTimes > 0 ){
            Double tps = requestTimes*1.0 / (System.currentTimeMillis() - lastCalculateTimestamp);
            lastCalculateTimestamp = 0L;
            return tps;
        }
        return 0d;
    }
    /**
     * inc 一次请求 kudu次数
     */
    public void incRequestTimes() {
        requestTimes += 1;
        if(lastCalculateTimestamp == 0 ){
            lastCalculateTimestamp = System.currentTimeMillis();
        }
    }

    /**
     * inc 一次请求 kudu次数
     */
    public void incTotalRequestTimes() {
        totalWordNums += 1;
    }
    public void initCacheSizeGauge(Gauge<Integer> gauge){
        metricGroup
                .addGroup(METRIC_GROUP)
                .gauge(CACHE_SIZE_METRIC_GAUGE, gauge);
    }
}
