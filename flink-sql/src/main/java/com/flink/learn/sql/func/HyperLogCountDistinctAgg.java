package com.flink.learn.sql.func;

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import org.apache.flink.table.functions.AggregateFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

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

    /**
     * 当开启 mini-batch的时候需要有merge
     * @param acc
     * @param it
     * @throws CardinalityMergeException
     */
    public void merge(HyperLogLog acc, Iterable<HyperLogLog> it) throws CardinalityMergeException {
        Iterator<HyperLogLog> iter = it.iterator();
        while (iter.hasNext()) {
            HyperLogLog a = iter.next();
            if(a != null) {
                acc.addAll(a);
            }
        }
    }
    public void accumulate(HyperLogLog acc,  String input) {
        acc.offer(input);
    }
}
