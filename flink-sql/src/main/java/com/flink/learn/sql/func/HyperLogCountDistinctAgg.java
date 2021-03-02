package com.flink.learn.sql.func;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.flink.table.functions.AggregateFunction;

import java.io.IOException;
import java.io.Serializable;

public class HyperLogCountDistinctAgg extends AggregateFunction<Long, HyperLogLog> {

    @Override
    public Long getValue(HyperLogLog hyperLogLog) {
        return hyperLogLog.cardinality();
    }

    @Override
    public HyperLogLog createAccumulator() {
        int log2m = HyperLogLog.log2m(0.05);
        return new HyperLogLog(log2m, new RegisterSet(1 << log2m));
    }

    public void accumulate(HyperLogLog acc,  String input) {
        acc.offer(input);
    }
}
