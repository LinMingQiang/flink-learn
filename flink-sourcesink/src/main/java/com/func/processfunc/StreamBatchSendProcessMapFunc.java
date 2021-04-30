package com.func.processfunc;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 间断性地发送数据给下游
 * 例如我们说知道checkpointedfunction的snatephshate方法在checkpoint的时候可以做sink输出。
 * 但是没法做collect.out()往下游算子输出。
 * 例如：我们有个在做sum聚合的时候不输出，而是往下走做其他trans。但是由于数据量很大，不可能+1就输出一条
 * 例如 100亿条做wordcout，输出从1-100亿都会输出，一条输出一次。太大了，想要做到 定时输出。
 * snatephshate只能输出到外部系统
 */
public class StreamBatchSendProcessMapFunc extends ProcessFunction<String, String> {

    public StreamBatchSendProcessMapFunc(Long batchInterval) {
        this.batchInterval = batchInterval;
    }

    public Long batchInterval;
    public boolean hasRegisterTimeserver = false;
    // 这个是task共享，Operator state也是task共享
    public HashMap<String, Long> wcMap = new HashMap<>();

    @Override
    public void processElement(String word, Context ctx, Collector<String> out) throws Exception {
        Long count = 0L;
        if (wcMap.containsKey(word)) {
            count = wcMap.get(word);
        }
        count += 1;
        wcMap.put(word, count);

        if (!hasRegisterTimeserver) {
            ctx.timerService().registerProcessingTimeTimer(System.currentTimeMillis() + batchInterval);
            hasRegisterTimeserver = true;
        }

    }

    /**
     * 每个task各自的
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        for (String s : wcMap.keySet()) {
            out.collect(s + ": " + wcMap.get(s));
        }
        hasRegisterTimeserver = false;
    }
}
