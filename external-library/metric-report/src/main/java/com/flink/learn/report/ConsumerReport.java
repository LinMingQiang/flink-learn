package com.flink.learn.report;

import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * metrics.reporters: my-report
 * metrics.reporter.my-report.class: com.flink.learn.report.ConsumerReport
 * metrics.reporter.my-report.interval: 60000
 */
// cp /Users/eminem/workspace/flink/flink-learn/external-library/metric-report/target/metric-report-1.14.2.jar /Users/eminem/programe/flink-1.13.2/lib
public class ConsumerReport extends AbstractReporter implements Scheduled {
    public static Logger LOG = LoggerFactory.getLogger(ConsumerReport.class);
    @Override
    public String filterCharacters(String s) {
        return s;
    }

    /**
     * @param metricConfig 在flink-conf.yaml 里面配置
     */
    @Override
    public void open(MetricConfig metricConfig) {

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
        Map<String, String> allVariables = group.getAllVariables();
        allVariables.entrySet().stream().forEach(ele -> {
//            if ("<job_name>".equalsIgnoreCase(ele.getKey())) {
//                System.out.printf(ele.getValue());
//            }
//            LOG.info("[notifyOfAddedMetric] metric={}; key={}; value={}", metricName, ele.getKey(), ele.getValue());
        });
        super.notifyOfAddedMetric(metric, metricName, group);
    }

    @Override
    public void report() {
        for (Map.Entry<Counter, String> metric : counters.entrySet()) {
            LOG.info("[Origin Counters] " + metric.getValue() + ": " + metric.getKey().getCount());
        }
        // 系统自带： localhost.jobmanager.Status.JVM.Memory.Direct.Count  : 11
        // 自定义 ： localhost.taskmanager.localhost:52930-e9b055.WordCountJobName.Flat Map.0
        //          .wordCountMetric.totalWordNums:2
        // wordCountMetric : 自定义的 addgroup ， totalWordNums 自定义的 gauge name
        // metric.getKey() 就是你的 值了。
        for (Map.Entry<Gauge<?>, String> metric : gauges.entrySet()) {
            if(metric.getValue().contains("Metric")) {
                LOG.info("[Origin Guages] " + metric.getValue() + ":" + metric.getKey().getValue());
            }
        }
        for (Map.Entry<Meter, String> metric : meters.entrySet()) {
            LOG.info("[Origin Meters] " + metric.getValue() + ": count=" + metric.getKey().getCount() + ",rate=" + metric.getKey().getRate());
        }
        for (Map.Entry<Histogram, String> metric : histograms.entrySet()) {
            LOG.info("[Origin histograms] " + metric.getValue() + ": max=" + metric.getKey().getStatistics().getMax()
                    + ",min=" + metric.getKey().getStatistics().getMin()
                    + ",mean=" + metric.getKey().getStatistics().getMean());
        }
    }
}
